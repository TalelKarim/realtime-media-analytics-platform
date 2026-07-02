locals {
  bronze_s3_location = "s3://${var.datalake_bucket_name}/bronze/wikimedia/recentchange/"
  silver_s3_location = "s3://${var.datalake_bucket_name}/silver/wikimedia/recentchange/"

  payload_type = "struct<wikimedia_recentchange_id:bigint,wiki:string,domain:string,stream:string,request_id:string,topic:string,partition:int,offset:bigint,change_type:string,namespace:int,title:string,title_url:string,user:string,user_is_bot:boolean,bot:boolean,is_minor:boolean,minor:boolean,is_patrolled:boolean,patrolled:boolean,comment:string,parsedcomment:string,source_timestamp:bigint,old_length:bigint,new_length:bigint,delta_bytes:bigint,length_old:bigint,length_new:bigint,length_delta:bigint,revision_old:bigint,revision_new:bigint,change_url:string,raw_notify_url:string,server_url:string,server_name:string,server_script_path:string,log_type:string,log_action:string>"

  raw_event_type = "struct<meta:struct<uri:string,request_id:string,id:string,domain:string,stream:string,dt:string,topic:string,partition:int,offset:bigint>,id:bigint,type:string,namespace:int,title:string,title_url:string,comment:string,timestamp:bigint,user:string,bot:boolean,notify_url:string,minor:boolean,patrolled:boolean,server_url:string,server_name:string,server_script_path:string,wiki:string,parsedcomment:string,log_id:bigint,log_type:string,log_action:string,log_action_comment:string>"
}

resource "aws_glue_catalog_database" "this" {
  name        = var.database_name
  description = "Data Catalog database for Realtime Media Analytics Platform."

  tags = var.tags
}

resource "aws_glue_catalog_table" "bronze_recentchange" {
  name          = "wikimedia_bronze_recentchange"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  description = "Bronze Wikimedia recentchange envelopes delivered by Firehose as JSON Lines GZIP."

  parameters = {
    EXTERNAL        = "TRUE"
    classification  = "json"
    compressionType = "gzip"
    typeOfData      = "file"

    # Athena partition projection.
    # $${year} means: keep ${year} for Athena, do not interpolate it in Terraform.
    "projection.enabled" = "true"

    "projection.year.type"   = "integer"
    "projection.year.range"  = "2026,2030"
    "projection.year.digits" = "4"

    "projection.month.type"   = "integer"
    "projection.month.range"  = "1,12"
    "projection.month.digits" = "2"

    "projection.day.type"   = "integer"
    "projection.day.range"  = "1,31"
    "projection.day.digits" = "2"

    "projection.hour.type"   = "integer"
    "projection.hour.range"  = "0,23"
    "projection.hour.digits" = "2"

    "storage.location.template" = "s3://${var.datalake_bucket_name}/bronze/wikimedia/recentchange/year=$${year}/month=$${month}/day=$${day}/hour=$${hour}/"
  }

  storage_descriptor {
    location      = local.bronze_s3_location
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "json-serde"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"

      parameters = {
        "ignore.malformed.json" = "true"
        "case.insensitive"      = "true"
      }
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "event_type"
      type = "string"
    }

    columns {
      name = "event_version"
      type = "string"
    }

    columns {
      name = "source"
      type = "string"
    }

    columns {
      name = "occurred_at"
      type = "string"
    }

    columns {
      name = "ingested_at"
      type = "string"
    }

    columns {
      name = "correlation_id"
      type = "string"
    }

    columns {
      name = "payload"
      type = local.payload_type
    }

    columns {
      name = "raw_event"
      type = local.raw_event_type
    }
  }

  partition_keys {
    name = "year"
    type = "int"
  }

  partition_keys {
    name = "month"
    type = "int"
  }

  partition_keys {
    name = "day"
    type = "int"
  }

  partition_keys {
    name = "hour"
    type = "int"
  }
}


resource "aws_glue_catalog_table" "silver_recentchange" {
  name          = "wikimedia_silver_recentchange"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  description = "Silver Wikimedia recentchange events cleaned and stored as Parquet SNAPPY."

  parameters = {
    EXTERNAL        = "TRUE"
    classification  = "parquet"
    compressionType = "snappy"
    typeOfData      = "file"

    "projection.enabled" = "true"

    "projection.ingestion_date.type"     = "date"
    "projection.ingestion_date.range"    = "2026-01-01,NOW"
    "projection.ingestion_date.format"   = "yyyy-MM-dd"
    "projection.ingestion_date.interval" = "1"
    "projection.ingestion_date.interval.unit" = "DAYS"

    "storage.location.template" = "s3://${var.datalake_bucket_name}/silver/wikimedia/recentchange/ingestion_date=$${ingestion_date}/"
  }

  storage_descriptor {
    location      = local.silver_s3_location
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "parquet-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "occurred_at"
      type = "timestamp"
    }

    columns {
      name = "wiki"
      type = "string"
    }

    columns {
      name = "domain"
      type = "string"
    }

    columns {
      name = "change_type"
      type = "string"
    }

    columns {
      name = "namespace"
      type = "int"
    }

    columns {
      name = "title"
      type = "string"
    }

    columns {
      name = "title_url"
      type = "string"
    }

    columns {
      name = "user"
      type = "string"
    }

    columns {
      name = "user_is_bot"
      type = "boolean"
    }

    columns {
      name = "is_minor"
      type = "boolean"
    }

    columns {
      name = "is_patrolled"
      type = "boolean"
    }

    columns {
      name = "old_length"
      type = "bigint"
    }

    columns {
      name = "new_length"
      type = "bigint"
    }

    columns {
      name = "delta_bytes"
      type = "bigint"
    }

    columns {
      name = "revision_old"
      type = "bigint"
    }

    columns {
      name = "revision_new"
      type = "bigint"
    }

    columns {
      name = "change_url"
      type = "string"
    }

    columns {
      name = "raw_notify_url"
      type = "string"
    }

    columns {
      name = "log_type"
      type = "string"
    }

    columns {
      name = "log_action"
      type = "string"
    }

    columns {
      name = "log_params"
      type = "string"
    }

    columns {
      name = "wikimedia_rcid"
      type = "bigint"
    }
  }

  partition_keys {
    name = "ingestion_date"
    type = "date"
  }
}