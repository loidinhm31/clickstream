declare module 'parquetjs-lite' {
  export class ParquetReader {
    static openFile(path: string): Promise<ParquetReader>;
    getCursor(columnList?: string[][]): ParquetCursor;
    close(): Promise<void>;
  }

  export class ParquetCursor {
    next(): Promise<Record<string, unknown> | null>;
  }
}
