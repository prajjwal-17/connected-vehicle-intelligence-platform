import { createClient, type RedisClientType } from 'redis';

export function idempotencyKey(eventId: string): string {
  return `telemetry:idempotency:${eventId}`;
}

export class RedisIdempotencyStore {
  private readonly client: RedisClientType;

  constructor(
    private readonly url: string,
    private readonly ttlSeconds: number,
  ) {
    this.client = createClient({ url });
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) await this.client.connect();
  }

  async claim(eventId: string): Promise<boolean> {
    await this.connect();
    const result = await this.client.set(idempotencyKey(eventId), '1', {
      NX: true,
      EX: this.ttlSeconds,
    });
    return result === 'OK';
  }

  async release(eventId: string): Promise<void> {
    await this.connect();
    await this.client.del(idempotencyKey(eventId));
  }

  async disconnect(): Promise<void> {
    if (this.client.isOpen) await this.client.quit();
  }
}
