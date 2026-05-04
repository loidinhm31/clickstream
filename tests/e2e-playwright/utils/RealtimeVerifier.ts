import axios from 'axios';
import { config } from './config';

export interface RealtimeStats {
  activeWebSocketSessions: number;
  batchCount: number;
  [key: string]: unknown;
}

export class RealtimeVerifier {
  private baseUrl: string;

  constructor() {
    this.baseUrl = config.realtimeApiUrl;
  }

  async getStats(): Promise<RealtimeStats | null> {
    try {
      const response = await axios.get<RealtimeStats>(`${this.baseUrl}/api/realtime/stats`);
      return response.data;
    } catch (error) {
      console.error('Failed to fetch realtime stats:', error);
      return null;
    }
  }

  /**
   * Verifies the health of the realtime service.
   */
  async checkHealth(): Promise<any> {
    try {
      const response = await axios.get(`${this.baseUrl}/api/realtime/health`);
      return response.data;
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      return { status: 'DOWN', error: msg };
    }
  }
}
