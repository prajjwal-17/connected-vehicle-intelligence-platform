import { describe, expect, it } from 'vitest';
import {
  StreamRedisIdempotencyStore,
  mapStreamIdempotencyResults,
  streamIdempotencyKey,
} from '../src/idempotency.js';
import { processStreamEventsInOrder } from '../src/consumer.js';

function fakeRedis(result: unknown[] | (() => unknown[] | Promise<unknown[]>)) {
  const commands: string[] = [];
  const client = {
    isOpen: false,
    connect: async () => undefined,
    quit: async () => undefined,
    del: async () => 1,
    multi: () => {
      const pipeline = {
        set: (key: string) => {
          commands.push(key);
          return pipeline;
        },
        exec: async () => (typeof result === 'function' ? result() : result),
      };
      return pipeline;
    },
  };
  return { client, commands };
}

describe('stream Redis idempotency pipelining', () => {
  it('claims all-new events in input order', async () => {
    const fake = fakeRedis(['OK', 'OK', 'OK']);
    const store = new StreamRedisIdempotencyStore('redis://test', 60, fake.client as never);
    await expect(store.claimMany(['a', 'b', 'c'])).resolves.toEqual([true, true, true]);
    expect(fake.commands).toEqual([
      streamIdempotencyKey('a'),
      streamIdempotencyKey('b'),
      streamIdempotencyKey('c'),
    ]);
  });

  it('rejects all duplicate events', async () => {
    const store = new StreamRedisIdempotencyStore(
      'redis://test',
      60,
      fakeRedis([null, null]).client as never,
    );
    await expect(store.claimMany(['a', 'b'])).resolves.toEqual([false, false]);
  });

  it('maps mixed new and duplicate replies to matching events', () => {
    expect(mapStreamIdempotencyResults(['OK', null, 'OK'])).toEqual([true, false, true]);
  });

  it('surfaces a pipeline failure without claiming success', async () => {
    const store = new StreamRedisIdempotencyStore(
      'redis://test',
      60,
      fakeRedis(() => {
        throw new Error('Redis pipeline failed');
      }).client as never,
    );
    await expect(store.claimMany(['a', 'b'])).rejects.toThrow('Redis pipeline failed');
  });

  it('preserves result-to-event mapping and partition order', async () => {
    const events = [{ eventId: 'a' }, { eventId: 'b' }, { eventId: 'c' }];
    const processed: string[] = [];
    await processStreamEventsInOrder(events, [true, false, true], async (event) => {
      processed.push(event.eventId);
    });
    expect(processed).toEqual(['a', 'c']);
  });

  it('preserves ordering when every event is new', async () => {
    const processed: string[] = [];
    await processStreamEventsInOrder(
      [{ eventId: '1' }, { eventId: '2' }, { eventId: '3' }],
      [true, true, true],
      async (event) => {
        processed.push(event.eventId);
      },
    );
    expect(processed).toEqual(['1', '2', '3']);
  });

  it('allows a retry after a temporary pipeline failure', async () => {
    let attempt = 0;
    const store = new StreamRedisIdempotencyStore(
      'redis://test',
      60,
      fakeRedis(() => {
        attempt += 1;
        if (attempt === 1) throw new Error('temporary failure');
        return ['OK', 'OK'];
      }).client as never,
    );
    await expect(store.claimMany(['a', 'b'])).rejects.toThrow('temporary failure');
    await expect(store.claimMany(['a', 'b'])).resolves.toEqual([true, true]);
  });

  it('does not process later offsets after a processing failure', async () => {
    const processed: string[] = [];
    await expect(
      processStreamEventsInOrder(
        [{ eventId: 'first' }, { eventId: 'second' }],
        [true, true],
        async (event) => {
          processed.push(event.eventId);
          if (event.eventId === 'first') throw new Error('processing failed');
        },
      ),
    ).rejects.toThrow('processing failed');
    expect(processed).toEqual(['first']);
  });
});
