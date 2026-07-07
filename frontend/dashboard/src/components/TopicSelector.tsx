import { Plus, X } from 'lucide-react';
import { useState } from 'react';
import { shortTopic } from '../lib/format';

interface TopicSelectorProps {
  activeTopics: string[];
  selectedTopic: string;
  onSelectedTopicChange: (topic: string) => void;
  onSubscribe: (topic: string) => void;
  onUnsubscribe: (topic: string) => void;
}

const SUGGESTED_TOPICS = ['global', 'top_pages', 'wiki:frwiki', 'wiki:enwiki', 'wiki:commonswiki', 'wiki:wikidatawiki'];

export const TopicSelector = ({
  activeTopics,
  selectedTopic,
  onSelectedTopicChange,
  onSubscribe,
  onUnsubscribe,
}: TopicSelectorProps) => {
  const [customTopic, setCustomTopic] = useState('');

  const submit = () => {
    const topic = customTopic.trim();
    if (!topic) return;
    onSubscribe(topic);
    onSelectedTopicChange(topic);
    setCustomTopic('');
  };

  return (
    <section className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 backdrop-blur">
      <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-white">Subscriptions</h2>
          <p className="text-sm text-slate-400">Subscribe to live topics and switch the dashboard view.</p>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        {activeTopics.map((topic) => (
          <button
            key={topic}
            type="button"
            onClick={() => onSelectedTopicChange(topic)}
            className={`group inline-flex items-center gap-2 rounded-full px-3 py-2 text-sm font-medium ring-1 transition ${
              selectedTopic === topic
                ? 'bg-sky-500 text-white ring-sky-400'
                : 'bg-slate-900 text-slate-300 ring-slate-800 hover:bg-slate-800'
            }`}
          >
            {shortTopic(topic)}
            <span
              role="button"
              tabIndex={0}
              onClick={(event) => {
                event.stopPropagation();
                onUnsubscribe(topic);
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.stopPropagation();
                  onUnsubscribe(topic);
                }
              }}
              className="rounded-full p-1 opacity-70 hover:bg-black/20 hover:opacity-100"
              aria-label={`Unsubscribe from ${topic}`}
            >
              <X className="h-3 w-3" />
            </span>
          </button>
        ))}
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-[1fr_auto]">
        <input
          value={customTopic}
          onChange={(event) => setCustomTopic(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') submit();
          }}
          placeholder="Add topic, e.g. wiki:arwiki"
          className="rounded-2xl border border-slate-800 bg-slate-900 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-600 focus:border-sky-500"
        />
        <button
          type="button"
          onClick={submit}
          className="inline-flex items-center justify-center gap-2 rounded-2xl bg-sky-500 px-4 py-3 text-sm font-semibold text-white shadow-lg shadow-sky-500/20 transition hover:bg-sky-400"
        >
          <Plus className="h-4 w-4" />
          Subscribe
        </button>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        {SUGGESTED_TOPICS.map((topic) => (
          <button
            key={topic}
            type="button"
            onClick={() => {
              onSubscribe(topic);
              onSelectedTopicChange(topic);
            }}
            className="rounded-full bg-slate-900 px-3 py-1 text-xs font-medium text-slate-400 ring-1 ring-slate-800 transition hover:bg-slate-800 hover:text-slate-200"
          >
            {topic}
          </button>
        ))}
      </div>
    </section>
  );
};
