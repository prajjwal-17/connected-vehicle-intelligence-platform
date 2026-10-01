import type { Logger } from 'pino';
import type { TelemetryEvent } from '@fleetpulse/schemas';
import { KafkaTelemetryProducer, type KafkaRuntimeConfig } from './kafka.js';

export class KafkaEventSink {
  private readonly producer: KafkaTelemetryProducer;

  constructor(config: KafkaRuntimeConfig, logger: Logger<never, boolean>) {
    this.producer = new KafkaTelemetryProducer(config, logger);
  }

  async write(event: TelemetryEvent): Promise<void> {
    await this.producer.send(event);
  }

  async close(): Promise<void> {
    await this.producer.disconnect();
  }
}
