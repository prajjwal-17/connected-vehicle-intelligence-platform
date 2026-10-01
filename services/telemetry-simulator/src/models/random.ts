export class SeededRandom {
  private state: number;

  constructor(seed: number) {
    this.state = seed | 0 || 1;
  }

  next(): number {
    let x = this.state;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.state = x | 0;
    return (this.state >>> 0) / 4_294_967_296;
  }

  between(min: number, max: number): number {
    return min + (max - min) * this.next();
  }

  integer(min: number, maxExclusive: number): number {
    return Math.floor(this.between(min, maxExclusive));
  }

  pick<T>(values: readonly T[]): T {
    return values[this.integer(0, values.length)];
  }
}
