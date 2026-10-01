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
};

export type KafkaMetrics = {
  telemetryEventsProduced: number;
  kafkaProducerErrors: number;
};

export type KafkaTelemetryMessage = {
  key: string;
  value: string;
  headers: { 'schema-version': string; 'event-type': string };
};

export function buildKafkaMessage(event: TelemetryEvent): KafkaTelemetryMessage {
  const validated = telemetryEventSchema.parse(event);
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
  readonly metrics: KafkaMetrics = { telemetryEventsProduced: 0, kafkaProducerErrors: 0 };
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private connected = false;

  constructor(
    private readonly config: KafkaRuntimeConfig,
    private readonly logger: Logger<never, boolean>,
  ) {
    this.kafka = createKafka(config);
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: false,
      maxInFlightRequests: 5,
    });
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    const admin = this.kafka.admin();
    await ensureTelemetryTopic(admin, this.config, this.logger);
    await admin.disconnect();
    await this.producer.connect();
    this.connected = true;
  }

  async send(event: TelemetryEvent): Promise<void> {
    const validated = telemetryEventSchema.parse(event);
    await this.connect();
    try {
      await this.producer.send({
        topic: this.config.kafkaTopic,
        acks: -1,
        compression: 1,
        messages: [buildKafkaMessage(validated)],
      });
      this.metrics.telemetryEventsProduced += 1;
    } catch (error) {
      this.metrics.kafkaProducerErrors += 1;
      this.logger.error(
        { err: error, eventId: validated.eventId },
        'kafka telemetry publish failed',
      );
      throw error;
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
