export class NotFoundError extends Error {
  readonly statusCode = 404;

  constructor(resource: string, id: string) {
    super(`${resource} with id ${id} was not found`);
    this.name = 'NotFoundError';
  }
}
