import { MongoClient } from 'mongodb';
import { config } from './config';

export class MongoVerifier {
  private client: MongoClient;
  private dbName: string;

  constructor() {
    const uri = config.mongoUri;
    this.client = new MongoClient(uri);
    this.dbName = uri.split('/').pop()?.split('?')[0] ?? 'clickstream_db';
  }

  async connect() {
    await this.client.connect();
  }

  async disconnect() {
    await this.client.close();
  }

  /**
   * Polls the session_aggregates collection until a session with the given ID appears.
   * @param sessionId The session ID to look for.
   * @param timeout Timeout in milliseconds (default 60s).
   * @returns The session aggregate document or null if not found.
   */
  async waitForSessionAggregate(sessionId: string, timeout: number = 60000): Promise<any> {
    const db = this.client.db(this.dbName);
    const collection = db.collection('session_aggregates');
    
    const startTime = Date.now();
    while (Date.now() - startTime < timeout) {
      const session = await collection.findOne({ sessionId });
      if (session) {
        return session;
      }
      await new Promise(resolve => setTimeout(resolve, 2000)); // Poll every 2s
    }
    
    return null;
  }

}
