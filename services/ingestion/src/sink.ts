import type { Logger } from 'pino';
import type { TelemetryEvent } from '@fleetpulse/schemas';
import { KafkaTelemetryProducer, type KafkaMetrics, type KafkaRuntimeConfig } from './kafka.js';

export class KafkaEventSink {
  private readonly producer: KafkaTelemetryProducer;
  private readonly pending: TelemetryEvent[] = [];
  private readonly inFlight = new Set<Promise<void>>();
  private pendingBytes = 0;
  private failure: unknown;
  private readonly batchSize: number;
  private readonly batchMaxBytes: number;
  private readonly maxInFlight: number;

  constructor(config: KafkaRuntimeConfig, logger: Logger<never, boolean>) {
    this.producer = new KafkaTelemetryProducer(config, logger);
    this.batchSize = config.kafkaBatchSize ?? 100;
    this.batchMaxBytes = config.kafkaBatchMaxBytes ?? 1_000_000;
    this.maxInFlight = config.kafkaBatchConcurrency ?? 1;
  }

  getMetrics(): KafkaMetrics {
    return this.producer.metrics;
  }

  async write(event: TelemetryEvent): Promise<void> {
    if (this.failure) throw this.failure;
    this.pending.push(event);
    this.pendingBytes += Buffer.byteLength(JSON.stringify(event));
    this.producer.metrics.queueDepthPeak = Math.max(
      this.producer.metrics.queueDepthPeak,
      this.pending.length,
    );
    if (this.pending.length >= this.batchSize || this.pendingBytes >= this.batchMaxBytes)
      this.startFlush();
    if (this.inFlight.size >= this.maxInFlight) await Promise.race(this.inFlight);
  }

  async close(): Promise<void> {
    this.startFlush();
    await Promise.all(this.inFlight);
    await this.producer.disconnect();
  }

  private startFlush(): void {
    if (this.pending.length === 0) return;
    const batch = this.pending.splice(0, this.pending.length);
    this.pendingBytes = 0;
    const request = this.producer.sendBatch(batch).catch((error) => {
      this.failure = error;
      throw error;
    });
    this.inFlight.add(request);
    this.producer.metrics.queueDepthPeak = Math.max(
      this.producer.metrics.queueDepthPeak,
      this.inFlight.size,
    );
    void request.then(
      () => this.inFlight.delete(request),
      () => this.inFlight.delete(request),
    );
  }
}
