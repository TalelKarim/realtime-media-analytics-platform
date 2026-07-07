import type { StatsSnapshot } from '../types/realtime';

export const demoStats: StatsSnapshot = {
  topic: 'global',
  receivedAt: new Date().toISOString(),
  eventCount: 1248,
  botCount: 421,
  humanCount: 827,
  topWikis: [
    { name: 'commonswiki', count: 426 },
    { name: 'wikidatawiki', count: 258 },
    { name: 'enwiki', count: 173 },
    { name: 'frwiki', count: 88 },
    { name: 'dewiki', count: 69 },
    { name: 'arwiki', count: 45 },
  ],
  changeTypes: [
    { name: 'edit', count: 636 },
    { name: 'categorize', count: 412 },
    { name: 'log', count: 118 },
    { name: 'new', count: 82 },
  ],
  namespaces: [
    { name: '14', count: 455 },
    { name: '0', count: 317 },
    { name: '6', count: 226 },
    { name: '1', count: 97 },
    { name: '10', count: 56 },
  ],
  topPages: [
    { wiki: 'commonswiki', title: 'Category:Images from Wikimedia Commons', namespace: 14, count: 18, botCount: 6, humanCount: 12 },
    { wiki: 'wikidatawiki', title: 'Q140399609', namespace: 0, count: 14, botCount: 8, humanCount: 6 },
    { wiki: 'enwiki', title: '2026 FIFA World Cup', namespace: 0, count: 11, botCount: 2, humanCount: 9 },
    { wiki: 'frwiki', title: 'Paris', namespace: 0, count: 7, botCount: 1, humanCount: 6 },
  ],
};
