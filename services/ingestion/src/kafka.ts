import { Kafka, type KafkaConfig, type Producer, type Admin, type Consumer } from 'kafkajs';
import type { Logger } from 'pino';
import { telemetryEventSchema, type TelemetryEvent } from '@fleetpulse/schemas';

export type KafkaRuntimeConfig = {
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaClientId: string;
  kafkaPartitions: number;
  kafkaReplicationFactor: number;
  kafkaRetentionMs: number;
  kafkaBatchSize?: number;
  kafkaBatchMaxBytes?: number;
  kafkaMaxInFlightRequests?: number;
  kafkaBatchConcurrency?: number;
  kafkaCompression?: boolean;
};

export type KafkaMetrics = {
  telemetryEventsProduced: number;
  kafkaProducerErrors: number;
  schemaValidationMs: number;
  serializationMs: number;
  kafkaSendWaitMs: number;
  batchesSent: number;
  successfulSends: number;
  failedSends: number;
  sendLatencyMs: number[];
  batchSizes: number[];
  inFlightPeak: number;
  activeSendRequests: number;
  queueDepthPeak: number;
};

export type KafkaTelemetryMessage = {
  key: string;
  value: string;
  headers: { 'schema-version': string; 'event-type': string };
};

export function buildKafkaMessage(event: TelemetryEvent): KafkaTelemetryMessage {
  return buildKafkaMessageFromValidated(telemetryEventSchema.parse(event));
}

function buildKafkaMessageFromValidated(validated: TelemetryEvent): KafkaTelemetryMessage {
  return {
    key: validated.vehicleId,
    value: JSON.stringify(validated),
    headers: {
      'schema-version': validated.schemaVersion,
      'event-type': validated.eventType,
    },
  };
}

export function createKafka(config: KafkaRuntimeConfig): Kafka {
  const kafkaConfig: KafkaConfig = {
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers,
    retry: { retries: 8, initialRetryTime: 100, maxRetryTime: 3_000 },
  };
  return new Kafka(kafkaConfig);
}

export async function ensureTelemetryTopic(
  admin: Admin,
  config: KafkaRuntimeConfig,
  logger: Logger<never, boolean>,
): Promise<void> {
  await admin.connect();
  try {
    const topics = await admin.listTopics();
    if (topics.includes(config.kafkaTopic)) {
      logger.info(
        { topic: config.kafkaTopic, partitions: config.kafkaPartitions },
        'kafka telemetry topic ready',
      );
      return;
    }
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: config.kafkaTopic,
          numPartitions: config.kafkaPartitions,
          replicationFactor: config.kafkaReplicationFactor,
          configEntries: [{ name: 'retention.ms', value: String(config.kafkaRetentionMs) }],
        },
      ],
    });
  } catch (error) {
    if ((error as { type?: string }).type !== 'TOPIC_ALREADY_EXISTS') throw error;
  }
  logger.info(
    { topic: config.kafkaTopic, partitions: config.kafkaPartitions },
    'kafka telemetry topic ready',
  );
}

export class KafkaTelemetryProducer {
  readonly metrics: KafkaMetrics = {
    telemetryEventsProduced: 0,
    kafkaProducerErrors: 0,
    schemaValidationMs: 0,
    serializationMs: 0,
    kafkaSendWaitMs: 0,
    batchesSent: 0,
    successfulSends: 0,
    failedSends: 0,
    sendLatencyMs: [],
    batchSizes: [],
    inFlightPeak: 0,
    activeSendRequests: 0,
    queueDepthPeak: 0,
  };
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private connected = false;
  private connectPromise?: Promise<void>;

  constructor(
    private readonly config: KafkaRuntimeConfig,
    private readonly logger: Logger<never, boolean>,
  ) {
    this.kafka = createKafka(config);
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: false,
      maxInFlightRequests: config.kafkaMaxInFlightRequests ?? 1,
    });
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = (async () => {
      const admin = this.kafka.admin();
      await ensureTelemetryTopic(admin, this.config, this.logger);
      await admin.disconnect();
      await this.producer.connect();
      this.connected = true;
    })();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = undefined;
    }
  }

  async send(event: TelemetryEvent): Promise<void> {
    await this.sendBatch([event]);
  }

  async sendBatch(events: TelemetryEvent[]): Promise<void> {
    if (events.length === 0) return;
    const validationStarted = performance.now();
    const validated = events.map((event) => telemetryEventSchema.parse(event));
    this.metrics.schemaValidationMs += performance.now() - validationStarted;
    await this.connect();
    const serializationStarted = performance.now();
    const messages = validated.map(buildKafkaMessageFromValidated);
    this.metrics.serializationMs += performance.now() - serializationStarted;
    this.metrics.batchSizes.push(validated.length);
    this.metrics.activeSendRequests += 1;
    this.metrics.inFlightPeak = Math.max(
      this.metrics.inFlightPeak,
      this.metrics.activeSendRequests,
    );
    const sendStarted = performance.now();
    try {
      await this.producer.send({
        topic: this.config.kafkaTopic,
        acks: -1,
        compression: this.config.kafkaCompression === false ? 0 : 1,
        messages,
      });
      const sendLatency = performance.now() - sendStarted;
      this.metrics.kafkaSendWaitMs += sendLatency;
      this.metrics.sendLatencyMs.push(sendLatency);
      this.metrics.batchesSent += 1;
      this.metrics.successfulSends += 1;
      this.metrics.telemetryEventsProduced += validated.length;
    } catch (error) {
      this.metrics.failedSends += 1;
      this.metrics.kafkaSendWaitMs += performance.now() - sendStarted;
      this.metrics.kafkaProducerErrors += validated.length;
      this.logger.error(
        { err: error, eventCount: validated.length },
        'kafka telemetry publish failed',
      );
      throw error;
    } finally {
      this.metrics.activeSendRequests -= 1;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return;
    await this.producer.disconnect();
    this.connected = false;
  }
}

export type ConsumerRuntime = { kafka: Kafka; consumer: Consumer };

export function createConsumer(
  config: KafkaRuntimeConfig & { kafkaGroupId: string },
): ConsumerRuntime {
  const kafka = createKafka(config);
  return {
    kafka,
    consumer: kafka.consumer({ groupId: config.kafkaGroupId, allowAutoTopicCreation: false }),
  };
}
