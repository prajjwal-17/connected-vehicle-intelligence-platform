import { createClient, type RedisClientType } from 'redis';

export function idempotencyKey(eventId: string): string {
  return `telemetry:idempotency:${eventId}`;
}

export function mapIdempotencyResults(results: unknown[]): boolean[] {
  return results.map((result) => {
    if (result instanceof Error) throw result;
    if (Array.isArray(result) && result[0] instanceof Error) throw result[0];
    const reply = Array.isArray(result) ? result[1] : result;
    return reply === 'OK';
  });
}

export class RedisIdempotencyStore {
  constructor(
    url: string,
    private readonly ttlSeconds: number,
    private readonly client: RedisClientType = createClient({ url }),
  ) {}

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

  async claimMany(eventIds: string[]): Promise<boolean[]> {
    if (eventIds.length === 0) return [];
    await this.connect();
    const pipeline = this.client.multi();
    for (const eventId of eventIds)
      pipeline.set(idempotencyKey(eventId), '1', { NX: true, EX: this.ttlSeconds });
    const results = await pipeline.exec();
    return mapIdempotencyResults(results);
  }

  async release(eventId: string): Promise<void> {
    await this.connect();
    await this.client.del(idempotencyKey(eventId));
  }

  async disconnect(): Promise<void> {
    if (this.client.isOpen) await this.client.quit();
  }
}
