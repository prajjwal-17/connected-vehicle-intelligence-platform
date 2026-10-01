import { describe, expect, it } from 'vitest';
import { pageResult } from '../src/utils/pagination.js';

describe('keyset pagination', () => {
  it('returns a next cursor only when another page exists', () => {
    const result = pageResult([{ id: 'a' }, { id: 'b' }, { id: 'c' }], 2, (item) => item.id);

    expect(result.data).toEqual([{ id: 'a' }, { id: 'b' }]);
    expect(result.pagination).toEqual({ limit: 2, hasNextPage: true, nextCursor: 'b' });
  });
});
