import * as dotenv from 'dotenv';
import path from 'path';

// Load .env once; subsequent calls in other modules are no-ops (dotenv is idempotent)
dotenv.config({ path: path.resolve(__dirname, '../.env') });

export const config = {
  mongoUri: process.env.MONGODB_URI ?? 'mongodb://localhost:9055/clickstream_db',
  dataLakePath: process.env.DATA_LAKE_PATH ?? '../../../data-lake/raw-events/',
  realtimeApiUrl: process.env.REALTIME_API_URL ?? 'http://localhost:9052',
} as const;
