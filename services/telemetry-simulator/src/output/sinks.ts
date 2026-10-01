import { mkdir } from 'node:fs/promises';
import { createWriteStream, type WriteStream } from 'node:fs';
import { dirname } from 'node:path';
import type { TelemetryEvent } from '@fleetpulse/schemas';

export interface EventSink {
  write(event: TelemetryEvent): Promise<void>;
  close(): Promise<void>;
}

export class StdoutEventSink implements EventSink {
  async write(event: TelemetryEvent): Promise<void> {
    const line = `${JSON.stringify({ ...event, deliveryTimestamp: new Date().toISOString() })}\n`;
    if (process.stdout.write(line)) return;
    await new Promise<void>((resolve) => process.stdout.once('drain', resolve));
  }

  async close(): Promise<void> {}
}

export class JsonlEventSink implements EventSink {
  private readonly stream: WriteStream;
  private streamError: Error | null = null;
  private closed = false;

  private constructor(stream: WriteStream) {
    this.stream = stream;
    this.stream.on('error', (error) => {
      this.streamError = error;
    });
  }

  static async create(filePath: string): Promise<JsonlEventSink> {
    await mkdir(dirname(filePath), { recursive: true });
    return new JsonlEventSink(createWriteStream(filePath, { flags: 'w', encoding: 'utf8' }));
  }

  async write(event: TelemetryEvent): Promise<void> {
    if (this.closed) throw new Error('Cannot write to a closed JSONL sink');
    if (this.streamError) throw this.streamError;
    const line = `${JSON.stringify({ ...event, deliveryTimestamp: new Date().toISOString() })}\n`;
    if (this.stream.write(line)) return;
    await new Promise<void>((resolve) => {
      this.stream.once('drain', resolve);
    });
    if (this.streamError) throw this.streamError;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.streamError) throw this.streamError;
    await new Promise<void>((resolve, reject) => {
      this.stream.once('finish', resolve);
      this.stream.once('error', reject);
      if (this.streamError) reject(this.streamError);
      this.stream.end();
    });
  }
}

export class MemoryEventSink implements EventSink {
  readonly events: TelemetryEvent[] = [];

  async write(event: TelemetryEvent): Promise<void> {
    this.events.push({ ...event, deliveryTimestamp: new Date().toISOString() });
  }

  async close(): Promise<void> {}
}

export class BoundedEventSink implements EventSink {
  private readonly queue: TelemetryEvent[] = [];
  private drainPromise: Promise<void> | null = null;
  private closed = false;
  private readonly waiters: Array<() => void> = [];

  constructor(
    private readonly downstream: EventSink,
    private readonly capacity: number,
    private readonly onBackpressure: () => void = () => undefined,
  ) {}

  async write(event: TelemetryEvent): Promise<void> {
    while (this.queue.length >= this.capacity) {
      this.onBackpressure();
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    if (this.closed) throw new Error('Cannot write to a closed event sink');
    this.queue.push(event);
    void this.drain();
  }

  private drain(): Promise<void> {
    if (this.drainPromise) return this.drainPromise;
    this.drainPromise = (async () => {
      while (this.queue.length > 0) {
        const event = this.queue.shift();
        const waiter = this.waiters.shift();
        waiter?.();
        if (event) await this.downstream.write(event);
      }
    })().finally(() => {
      this.drainPromise = null;
    });
    return this.drainPromise;
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.drain();
    await this.downstream.close();
  }
}

export class OutOfOrderSink implements EventSink {
  private readonly pending: TelemetryEvent[] = [];

  constructor(
    private readonly downstream: EventSink,
    private readonly rate: number,
    private readonly maxDelayMs: number,
    private readonly random: () => number,
    private readonly onDelayed: () => void = () => undefined,
  ) {}

  async write(event: TelemetryEvent): Promise<void> {
    if (this.random() < this.rate) {
      this.pending.push(event);
      this.onDelayed();
      if (this.pending.length < 3) return;
      const reordered = this.pending.pop();
      if (reordered) await this.writeWithDeliveryTimestamp(reordered);
      return;
    }
    await this.writeWithDeliveryTimestamp(event);
  }

  private async writeWithDeliveryTimestamp(event: TelemetryEvent) {
    const deliveryTimestamp = new Date(
      Date.now() + Math.floor(this.random() * this.maxDelayMs),
    ).toISOString();
    await this.downstream.write({ ...event, deliveryTimestamp });
  }

  async close(): Promise<void> {
    while (this.pending.length > 0) {
      const event = this.pending.pop();
      if (event) await this.writeWithDeliveryTimestamp(event);
    }
    await this.downstream.close();
  }
}
