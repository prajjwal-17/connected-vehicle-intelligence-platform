import { createClient, type RedisClientType } from 'redis';

export function streamIdempotencyKey(eventId: string): string {
  return `stream:idempotency:${eventId}`;
}

export function mapStreamIdempotencyResults(results: unknown[]): boolean[] {
  return results.map((result) => {
    if (result instanceof Error) throw result;
    if (Array.isArray(result) && result[0] instanceof Error) throw result[0];
    const reply = Array.isArray(result) ? result[1] : result;
    return reply === 'OK';
  });
}

export class StreamRedisIdempotencyStore {
  constructor(
    url: string,
    private readonly ttlSeconds: number,
    private readonly client: RedisClientType = createClient({ url }),
  ) {}

  async connect(): Promise<void> {
    if (!this.client.isOpen) await this.client.connect();
  }

  async claimMany(eventIds: string[]): Promise<boolean[]> {
    if (eventIds.length === 0) return [];
    await this.connect();
    const pipeline = this.client.multi();
    for (const eventId of eventIds)
      pipeline.set(streamIdempotencyKey(eventId), '1', { NX: true, EX: this.ttlSeconds });
    return mapStreamIdempotencyResults(await pipeline.exec());
  }

  async release(eventId: string): Promise<void> {
    await this.connect();
    await this.client.del(streamIdempotencyKey(eventId));
  }

  async disconnect(): Promise<void> {
    if (this.client.isOpen) await this.client.quit();
  }
}
