import { tableFromIPC } from 'apache-arrow';
import type { Table } from 'apache-arrow';

interface TrendingPage {
  url: string;
  views: number;
}

export interface RealtimeMetrics {
  activeUsers: number;
  clicksPerSecond: number;
  eventRate: number;
  trendingPages: TrendingPage[];
}

export function decodeArrowMetrics(buffer: ArrayBuffer): RealtimeMetrics | null {
  if (!buffer || buffer.byteLength === 0) return null;
  
  try {
    const table: Table = tableFromIPC(new Uint8Array(buffer));

    if (table.numCols === 0) return null;

    // Extract metrics from Arrow table columns
    const metrics: RealtimeMetrics = {
      activeUsers: extractNumber(table, 'activeUsers', 0),
      clicksPerSecond: extractNumber(table, 'clicksPerSecond', 0),
      eventRate: extractNumber(table, 'eventRate', 0),
      trendingPages: extractTrendingPages(table),
    };

    return metrics;
  } catch (error) {
    console.error('Failed to decode Arrow IPC:', error);
    return null;
  }
}

function extractNumber(table: Table, columnName: string, defaultValue: number): number {
  try {
    const column = table.getChild(columnName);
    if (!column || column.length === 0) return defaultValue;
    const value = column.get(0);
    return typeof value === 'number' ? value : defaultValue;
  } catch {
    return defaultValue;
  }
}

function extractTrendingPages(table: Table): TrendingPage[] {
  try {
    const urlColumn = table.getChild('trendingPageUrl');
    const viewsColumn = table.getChild('trendingPageViews');

    if (!urlColumn || !viewsColumn) return [];

    const pages: TrendingPage[] = [];
    const length = Math.min(urlColumn.length, viewsColumn.length);

    for (let i = 0; i < length; i++) {
      const url = urlColumn.get(i);
      const views = viewsColumn.get(i);

      if (url && typeof views === 'number') {
        pages.push({ url: String(url), views });
      }
    }

    return pages;
  } catch {
    return [];
  }
}
