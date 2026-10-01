import { ZodError } from 'zod';

export type IngestionErrorClass = 'validation' | 'transient' | 'permanent';

export function classifyIngestionError(error: unknown): IngestionErrorClass {
  if (error instanceof ZodError || error instanceof SyntaxError) return 'validation';
  if (
    error instanceof Error &&
    /timeout|temporar|connection|network|redis|kafka/i.test(error.message)
  ) {
    return 'transient';
  }
  return 'permanent';
}
