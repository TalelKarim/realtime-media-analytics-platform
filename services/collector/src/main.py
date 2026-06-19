from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError


STOP_REQUESTED = False
LOGGER = logging.getLogger("wikimedia-collector")


@dataclass(frozen=True)
class CollectorConfig:
    aws_region: str
    wikimedia_stream_url: str
    kinesis_stream_name: Optional[str]
    sample_rate: float
    batch_size: int
    flush_interval_seconds: float
    log_level: str
    kinesis_max_retries: int
    kinesis_retry_base_sleep_seconds: float
    reconnect_sleep_seconds: float


@dataclass
class CollectorStats:
    received: int = 0
    parse_failed: int = 0
    invalid: int = 0
    canary_dropped: int = 0
    sampled_out: int = 0
    kept: int = 0
    dry_run_records: int = 0
    sent_to_kinesis: int = 0
    kinesis_failed: int = 0
    kinesis_flushes: int = 0
    reconnects: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(message)s",
        stream=sys.stdout,
        force=True,
    )

    # Avoid noisy dependency logs unless debugging deeply.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)


def log_event(level: int, message: str, **fields: Any) -> None:
    payload = {
        "ts": utc_now_iso(),
        "level": logging.getLevelName(level),
        "message": message,
        **fields,
    }
    LOGGER.log(level, json.dumps(payload, ensure_ascii=False, default=str))


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def load_config() -> CollectorConfig:
    config = CollectorConfig(
        aws_region=env_str("AWS_REGION", "us-east-1") or "us-east-1",
        wikimedia_stream_url=env_str(
            "WIKIMEDIA_STREAM_URL",
            "https://stream.wikimedia.org/v2/stream/recentchange",
        )
        or "https://stream.wikimedia.org/v2/stream/recentchange",
        kinesis_stream_name=env_str("KINESIS_STREAM_NAME"),
        sample_rate=env_float("SAMPLE_RATE", 0.01),
        batch_size=env_int("BATCH_SIZE", 100),
        flush_interval_seconds=env_float("FLUSH_INTERVAL_SECONDS", 2.0),
        log_level=env_str("LOG_LEVEL", "INFO") or "INFO",
        kinesis_max_retries=env_int("KINESIS_MAX_RETRIES", 3),
        kinesis_retry_base_sleep_seconds=env_float("KINESIS_RETRY_BASE_SLEEP_SECONDS", 0.5),
        reconnect_sleep_seconds=env_float("RECONNECT_SLEEP_SECONDS", 5.0),
    )

    if not 0.0 <= config.sample_rate <= 1.0:
        raise ValueError("SAMPLE_RATE must be between 0.0 and 1.0")

    if not 1 <= config.batch_size <= 500:
        raise ValueError("BATCH_SIZE must be between 1 and 500 for Kinesis PutRecords")

    if config.flush_interval_seconds <= 0:
        raise ValueError("FLUSH_INTERVAL_SECONDS must be greater than 0")

    if config.kinesis_max_retries < 0:
        raise ValueError("KINESIS_MAX_RETRIES must be greater than or equal to 0")

    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wikimedia EventStreams collector")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read, validate, sample and normalize events without sending to Kinesis.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after reading this number of raw data events. Useful for local tests.",
    )
    return parser.parse_args()


def handle_shutdown_signal(signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log_event(logging.WARNING, "shutdown_signal_received", signal=signum)


def parse_sse_data_line(line: str) -> Dict[str, Any]:
    """
    Wikimedia EventStreams uses Server-Sent Events.

    We only care about lines like:
      data: {...json...}

    This function removes 'data:' and parses the JSON payload.
    """
    if not line.startswith("data:"):
        raise ValueError("line_is_not_data_event")

    json_payload = line[len("data:") :].lstrip()

    if not json_payload:
        raise ValueError("empty_data_payload")

    parsed = json.loads(json_payload)

    if not isinstance(parsed, dict):
        raise ValueError("data_payload_is_not_json_object")

    return parsed


def extract_required_meta(raw_event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    meta = raw_event.get("meta")

    if not isinstance(meta, dict):
        return None, None, None, "missing_or_invalid_meta"

    meta_id = meta.get("id")
    meta_dt = meta.get("dt")

    if not meta_id:
        return None, None, meta, "missing_meta_id"

    if not meta_dt:
        return str(meta_id), None, meta, "missing_meta_dt"

    return str(meta_id), str(meta_dt), meta, None


def is_canary_event(meta: Dict[str, Any]) -> bool:
    return str(meta.get("domain", "")).lower() == "canary"


def sampling_score(sampling_key: str) -> float:
    """
    Deterministic sampling score.

    SHA-256 does NOT directly produce a number between 0 and 1.
    We:
      1. hash the sampling key,
      2. take the first 8 hex chars,
      3. convert them to an integer,
      4. divide by the max possible 8-hex-char value.

    Result: stable score in [0, 1].
    """
    digest = hashlib.sha256(sampling_key.encode("utf-8")).hexdigest()
    first_8_hex_chars = digest[:8]
    integer_value = int(first_8_hex_chars, 16)
    return integer_value / 0xFFFFFFFF


def should_sample(sampling_key: str, sample_rate: float) -> bool:
    if sample_rate >= 1.0:
        return True

    if sample_rate <= 0.0:
        return False

    return sampling_score(sampling_key) < sample_rate


def get_nested_dict_value(source: Dict[str, Any], parent_key: str, child_key: str) -> Any:
    parent = source.get(parent_key)
    if not isinstance(parent, dict):
        return None
    return parent.get(child_key)


def compute_length_delta(length_old: Any, length_new: Any) -> Optional[int]:
    if isinstance(length_old, int) and isinstance(length_new, int):
        return length_new - length_old
    return None


def build_change_url(raw_event: Dict[str, Any]) -> Optional[str]:
    meta = raw_event.get("meta")
    meta_uri = meta.get("uri") if isinstance(meta, dict) else None

    return (
        raw_event.get("notify_url")
        or raw_event.get("title_url")
        or meta_uri
    )


def normalize_event(raw_event: Dict[str, Any], event_id: str, occurred_at: str) -> Dict[str, Any]:
    meta = raw_event.get("meta")
    if not isinstance(meta, dict):
        meta = {}

    revision_old = get_nested_dict_value(raw_event, "revision", "old")
    revision_new = get_nested_dict_value(raw_event, "revision", "new")
    length_old = get_nested_dict_value(raw_event, "length", "old")
    length_new = get_nested_dict_value(raw_event, "length", "new")

    normalized_event = {
        "event_id": event_id,
        "event_type": "wiki.recentchange",
        "event_version": "1.0",
        "source": "wikimedia.eventstreams",
        "occurred_at": occurred_at,
        "ingested_at": utc_now_iso(),
        "correlation_id": str(uuid.uuid4()),
        "payload": {
            "wiki": raw_event.get("wiki"),
            "domain": meta.get("domain"),
            "stream": meta.get("stream"),
            "request_id": meta.get("request_id"),
            "topic": meta.get("topic"),
            "partition": meta.get("partition"),
            "offset": meta.get("offset"),
            "change_type": raw_event.get("type"),
            "namespace": raw_event.get("namespace"),
            "title": raw_event.get("title"),
            "title_url": raw_event.get("title_url"),
            "user": raw_event.get("user"),
            "bot": raw_event.get("bot"),
            "minor": raw_event.get("minor"),
            "patrolled": raw_event.get("patrolled"),
            "comment": raw_event.get("comment"),
            "parsedcomment": raw_event.get("parsedcomment"),
            "source_timestamp": raw_event.get("timestamp"),
            "revision_old": revision_old,
            "revision_new": revision_new,
            "length_old": length_old,
            "length_new": length_new,
            "length_delta": compute_length_delta(length_old, length_new),
            "change_url": build_change_url(raw_event),
            "server_url": raw_event.get("server_url"),
            "server_name": raw_event.get("server_name"),
            "server_script_path": raw_event.get("server_script_path"),
        },
        "raw_event": raw_event,
    }

    return normalized_event


def build_kinesis_record(normalized_event: Dict[str, Any]) -> Dict[str, Any]:
    event_id = normalized_event["event_id"]

    return {
        "Data": json.dumps(
            normalized_event,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"),
        "PartitionKey": event_id,
    }


def extract_failed_records(
    original_records: List[Dict[str, Any]],
    kinesis_response: Dict[str, Any],
) -> List[Dict[str, Any]]:
    failed_records: List[Dict[str, Any]] = []

    response_records = kinesis_response.get("Records", [])

    for index, record_response in enumerate(response_records):
        if "ErrorCode" in record_response:
            failed_records.append(original_records[index])

    return failed_records


def put_records_with_retries(
    kinesis_client: Any,
    stream_name: str,
    records: List[Dict[str, Any]],
    max_retries: int,
    retry_base_sleep_seconds: float,
) -> Tuple[int, int]:
    """
    Returns:
      (sent_successfully, failed_after_retries)
    """
    if not records:
        return 0, 0

    pending_records = records
    total_records = len(records)

    for attempt in range(max_retries + 1):
        try:
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=pending_records,
            )
        except (ClientError, BotoCoreError) as exc:
            if attempt >= max_retries:
                log_event(
                    logging.ERROR,
                    "kinesis_put_records_request_failed_after_retries",
                    error=str(exc),
                    attempted_records=len(pending_records),
                    attempt=attempt,
                )
                return 0, len(pending_records)

            sleep_seconds = retry_base_sleep_seconds * (2 ** attempt)
            log_event(
                logging.WARNING,
                "kinesis_put_records_request_failed_retrying",
                error=str(exc),
                attempted_records=len(pending_records),
                attempt=attempt,
                sleep_seconds=sleep_seconds,
            )
            time.sleep(sleep_seconds)
            continue

        failed_records = extract_failed_records(pending_records, response)
        failed_count = len(failed_records)

        if failed_count == 0:
            sent_successfully = total_records
            return sent_successfully, 0

        if attempt >= max_retries:
            sent_successfully = total_records - failed_count
            log_event(
                logging.ERROR,
                "kinesis_put_records_partial_failure_after_retries",
                sent_successfully=sent_successfully,
                failed_after_retries=failed_count,
                attempt=attempt,
            )
            return sent_successfully, failed_count

        sleep_seconds = retry_base_sleep_seconds * (2 ** attempt)

        log_event(
            logging.WARNING,
            "kinesis_put_records_partial_failure_retrying",
            failed_records=failed_count,
            attempt=attempt,
            sleep_seconds=sleep_seconds,
        )

        pending_records = failed_records
        time.sleep(sleep_seconds)

    # Defensive fallback. The loop should always return before this.
    return 0, len(records)


def flush_buffer_to_kinesis(
    buffer: List[Dict[str, Any]],
    kinesis_client: Any,
    config: CollectorConfig,
    stats: CollectorStats,
) -> None:
    if not buffer:
        return

    if not config.kinesis_stream_name:
        raise ValueError("KINESIS_STREAM_NAME is required when dry-run is disabled")

    records_to_send = list(buffer)
    buffer.clear()

    stats.kinesis_flushes += 1

    log_event(
        logging.INFO,
        "batch_flush_started",
        records=len(records_to_send),
        stream_name=config.kinesis_stream_name,
    )

    sent, failed = put_records_with_retries(
        kinesis_client=kinesis_client,
        stream_name=config.kinesis_stream_name,
        records=records_to_send,
        max_retries=config.kinesis_max_retries,
        retry_base_sleep_seconds=config.kinesis_retry_base_sleep_seconds,
    )

    stats.sent_to_kinesis += sent
    stats.kinesis_failed += failed

    if failed == 0:
        log_event(
            logging.INFO,
            "kinesis_put_records_success",
            sent=sent,
            failed=failed,
        )
    else:
        log_event(
            logging.ERROR,
            "kinesis_records_dropped_after_retries",
            sent=sent,
            failed=failed,
        )


def should_stop_for_max_events(stats: CollectorStats, max_events: Optional[int]) -> bool:
    return max_events is not None and stats.received >= max_events


def process_raw_event(
    raw_event: Dict[str, Any],
    config: CollectorConfig,
    stats: CollectorStats,
    dry_run: bool,
    buffer: List[Dict[str, Any]],
) -> None:
    meta_id, meta_dt, meta, validation_error = extract_required_meta(raw_event)

    if validation_error:
        stats.invalid += 1
        log_event(
            logging.WARNING,
            "event_invalid",
            reason=validation_error,
        )
        return

    assert meta_id is not None
    assert meta_dt is not None
    assert meta is not None

    if is_canary_event(meta):
        stats.canary_dropped += 1
        log_event(
            logging.DEBUG,
            "event_dropped_canary",
            meta_id=meta_id,
            domain=meta.get("domain"),
        )
        return

    event_id = f"wikimedia-{meta_id}"

    if not should_sample(event_id, config.sample_rate):
        stats.sampled_out += 1
        log_event(
            logging.DEBUG,
            "event_sampled_out",
            event_id=event_id,
            sample_rate=config.sample_rate,
        )
        return

    normalized_event = normalize_event(
        raw_event=raw_event,
        event_id=event_id,
        occurred_at=meta_dt,
    )

    stats.kept += 1

    if dry_run:
        stats.dry_run_records += 1

        if LOGGER.isEnabledFor(logging.DEBUG):
            log_event(
                logging.DEBUG,
                "dry_run_record",
                normalized_event=normalized_event,
            )
        else:
            log_event(
                logging.INFO,
                "dry_run_record",
                event_id=normalized_event["event_id"],
                wiki=normalized_event["payload"].get("wiki"),
                change_type=normalized_event["payload"].get("change_type"),
                title=normalized_event["payload"].get("title"),
                bot=normalized_event["payload"].get("bot"),
            )
        return

    buffer.append(build_kinesis_record(normalized_event))


def run_collector(config: CollectorConfig, args: argparse.Namespace) -> CollectorStats:
    stats = CollectorStats()
    buffer: List[Dict[str, Any]] = []
    last_flush_monotonic = time.monotonic()

    kinesis_client = None
    if not args.dry_run:
        if not config.kinesis_stream_name:
            raise ValueError("KINESIS_STREAM_NAME is required when dry-run is disabled")

        kinesis_client = boto3.client("kinesis", region_name=config.aws_region)

    session = requests.Session()

    log_event(
        logging.INFO,
        "collector_started",
        dry_run=args.dry_run,
        max_events=args.max_events,
        aws_region=config.aws_region,
        stream_url=config.wikimedia_stream_url,
        kinesis_stream_name=config.kinesis_stream_name,
        sample_rate=config.sample_rate,
        batch_size=config.batch_size,
        flush_interval_seconds=config.flush_interval_seconds,
    )

    while not STOP_REQUESTED and not should_stop_for_max_events(stats, args.max_events):
        try:
            log_event(
                logging.INFO,
                "wikimedia_stream_connecting",
                stream_url=config.wikimedia_stream_url,
            )

            with session.get(
                config.wikimedia_stream_url,
                headers={
                    "Accept": "text/event-stream",
                    "User-Agent": (
                        "realtime-media-analytics-platform/0.1 "
                        "(https://github.com/TalelKarim/realtime-media-analytics-platform; "
                        "learning-portfolio-project)"
                    ),
                },
                stream=True,
                timeout=(10, 90),
            ) as response:
                response.raise_for_status()

                log_event(
                    logging.INFO,
                    "wikimedia_stream_connected",
                    status_code=response.status_code,
                )

                for line in response.iter_lines(decode_unicode=True):
                    if STOP_REQUESTED or should_stop_for_max_events(stats, args.max_events):
                        break

                    if not line:
                        continue

                    if isinstance(line, bytes):
                        line = line.decode("utf-8")

                    if not line.startswith("data:"):
                        continue

                    # Flush by time before processing the next data event.
                    if (
                        not args.dry_run
                        and buffer
                        and time.monotonic() - last_flush_monotonic >= config.flush_interval_seconds
                    ):
                        assert kinesis_client is not None
                        flush_buffer_to_kinesis(
                            buffer=buffer,
                            kinesis_client=kinesis_client,
                            config=config,
                            stats=stats,
                        )
                        last_flush_monotonic = time.monotonic()

                    stats.received += 1

                    try:
                        raw_event = parse_sse_data_line(line)
                    except (json.JSONDecodeError, ValueError) as exc:
                        stats.parse_failed += 1
                        log_event(
                            logging.WARNING,
                            "json_parse_failed",
                            error=str(exc),
                        )
                        continue

                    process_raw_event(
                        raw_event=raw_event,
                        config=config,
                        stats=stats,
                        dry_run=args.dry_run,
                        buffer=buffer,
                    )

                    if (
                        not args.dry_run
                        and len(buffer) >= config.batch_size
                    ):
                        assert kinesis_client is not None
                        flush_buffer_to_kinesis(
                            buffer=buffer,
                            kinesis_client=kinesis_client,
                            config=config,
                            stats=stats,
                        )
                        last_flush_monotonic = time.monotonic()

                # If the stream loop exits because max-events/shutdown was reached,
                # we exit the outer reconnect loop too.
                if STOP_REQUESTED or should_stop_for_max_events(stats, args.max_events):
                    break

        except requests.RequestException as exc:
            stats.reconnects += 1
            log_event(
                logging.ERROR,
                "wikimedia_stream_connection_failed",
                error=str(exc),
                reconnect_sleep_seconds=config.reconnect_sleep_seconds,
                reconnects=stats.reconnects,
            )
            time.sleep(config.reconnect_sleep_seconds)

    # Final flush for any remaining records.
    if not args.dry_run and buffer:
        assert kinesis_client is not None
        flush_buffer_to_kinesis(
            buffer=buffer,
            kinesis_client=kinesis_client,
            config=config,
            stats=stats,
        )

    log_event(
        logging.INFO,
        "collector_summary",
        **asdict(stats),
        dry_run=args.dry_run,
    )

    return stats


def main() -> int:
    args = parse_args()
    config = load_config()

    setup_logging(config.log_level)

    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    try:
        run_collector(config, args)
        return 0
    except Exception as exc:
        log_event(
            logging.CRITICAL,
            "collector_crashed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())