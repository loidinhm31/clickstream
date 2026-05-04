import * as parquet from 'parquetjs-lite';
import * as fs from 'fs';
import * as path from 'path';
import { config } from './config';

export class ParquetVerifier {
  private dataLakePath: string;

  constructor() {
    this.dataLakePath = path.resolve(__dirname, config.dataLakePath);
  }

  /**
   * Scans the data lake for a session ID in Parquet files.
   * @param sessionId The session ID to look for.
   * @param timeout Timeout in milliseconds.
   */
  async waitForSessionInParquet(sessionId: string, timeout: number = 90000): Promise<boolean> {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeout) {
      const files = this.getRecentParquetFiles();
      
      for (const file of files) {
        const found = await this.searchSessionInFile(file, sessionId);
        if (found) {
          return true;
        }
      }
      
      await new Promise(resolve => setTimeout(resolve, 5000)); // Poll every 5s
    }
    
    return false;
  }

  private getRecentParquetFiles(): string[] {
    if (!fs.existsSync(this.dataLakePath)) {
      console.warn(`Data lake path does not exist: ${this.dataLakePath}`);
      return [];
    }

    // Recursively find all .parquet files
    const files: string[] = [];
    const scanDir = (dir: string) => {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanDir(fullPath);
        } else if (entry.name.endsWith('.parquet')) {
          files.push(fullPath);
        }
      }
    };

    scanDir(this.dataLakePath);
    
    // Sort by mtime descending
    return files.sort((a, b) => {
      return fs.statSync(b).mtime.getTime() - fs.statSync(a).mtime.getTime();
    });
  }

  private async searchSessionInFile(filePath: string, sessionId: string): Promise<boolean> {
    let reader: parquet.ParquetReader | undefined;
    try {
      reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      let record: Record<string, unknown> | null;
      while ((record = await cursor.next()) !== null) {
        if (record['sessionId'] === sessionId) {
          return true;
        }
      }
    } catch (error) {
      console.error(`Error reading parquet file ${filePath}:`, error);
    } finally {
      await reader?.close();
    }
    return false;
  }
}
