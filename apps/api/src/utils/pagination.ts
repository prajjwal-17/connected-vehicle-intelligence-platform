export const DEFAULT_PAGE_SIZE = 25;
export const MAX_PAGE_SIZE = 100;

export function pageResult<T>(items: T[], limit: number, getCursor: (item: T) => string) {
  const hasNextPage = items.length > limit;
  const pageItems = hasNextPage ? items.slice(0, limit) : items;

  return {
    data: pageItems,
    pagination: {
      limit,
      hasNextPage,
      nextCursor: hasNextPage ? getCursor(pageItems[pageItems.length - 1]) : null,
    },
  };
}
