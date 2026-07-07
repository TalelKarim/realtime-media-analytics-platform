import type { FormEvent } from 'react';
import { useState } from 'react';
import { Plus, X } from 'lucide-react';
import { SUGGESTED_TOPICS } from '../config';
import { shortTopic } from '../lib/format';

interface TopicSelectorProps {
  activeTopics: string[];
  selectedTopic: string;
  onSelectedTopicChange: (topic: string) => void;
  onSubscribe: (topic: string) => void;
  onUnsubscribe: (topic: string) => void;
}

export const TopicSelector = ({
  activeTopics,
  selectedTopic,
  onSelectedTopicChange,
  onSubscribe,
  onUnsubscribe,
}: TopicSelectorProps) => {
  const [draftTopic, setDraftTopic] = useState('');

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const topic = draftTopic.trim();
    if (!topic) return;

    onSubscribe(topic);
    onSelectedTopicChange(topic);
    setDraftTopic('');
  };

  return (
    <section className="rounded-3xl border border-slate-800/90 bg-slate-950/85 p-5 backdrop-blur">
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-white">Subscriptions</h2>
        <p className="text-sm text-slate-400">Subscribe to live topics and switch the dashboard view.</p>
      </div>

      <div className="mb-4 flex flex-wrap gap-2">
        {activeTopics.map((topic) => {
          const selected = topic === selectedTopic;
          return (
            <button
              key={topic}
              type="button"
              onClick={() => onSelectedTopicChange(topic)}
              className={`inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold transition ${
                selected
                  ? 'bg-sky-500 text-white shadow-lg shadow-sky-500/20'
                  : 'bg-slate-900 text-slate-300 ring-1 ring-slate-800 hover:bg-slate-800'
              }`}
              title={topic}
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
                    event.preventDefault();
                    event.stopPropagation();
                    onUnsubscribe(topic);
                  }
                }}
                className="rounded-full p-0.5 opacity-80 hover:bg-white/10 hover:opacity-100"
                aria-label={`Unsubscribe from ${topic}`}
              >
                <X className="h-3.5 w-3.5" />
              </span>
            </button>
          );
        })}
      </div>

      <form onSubmit={submit} className="flex flex-col gap-3 sm:flex-row">
        <input
          value={draftTopic}
          onChange={(event) => setDraftTopic(event.target.value)}
          placeholder="Add topic, e.g. wiki:arwiki"
          className="min-w-0 flex-1 rounded-2xl border border-slate-800 bg-slate-900/80 px-4 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
        />
        <button
          type="submit"
          className="inline-flex items-center justify-center gap-2 rounded-2xl bg-sky-500 px-5 py-3 text-sm font-semibold text-white shadow-lg shadow-sky-500/20 transition hover:bg-sky-400"
        >
          <Plus className="h-4 w-4" />
          Subscribe
        </button>
      </form>

      <div className="mt-3 flex flex-wrap gap-2">
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
