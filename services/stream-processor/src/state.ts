import type { TelemetryEvent } from '@fleetpulse/schemas';

export type VehicleState = {
  vehicleId: string;
  lastTimestampMs: number;
  events: TelemetryEvent[];
  normalCounts: Map<string, number>;
};

export class BoundedVehicleStateStore {
  private readonly states = new Map<string, VehicleState>();
  constructor(
    private readonly maxEvents: number,
    private readonly maxAgeMs: number,
  ) {}

  get(vehicleId: string): VehicleState | undefined {
    return this.states.get(vehicleId);
  }

  getOrCreate(vehicleId: string, timestampMs: number): VehicleState {
    const current = this.states.get(vehicleId) ?? {
      vehicleId,
      lastTimestampMs: timestampMs,
      events: [],
      normalCounts: new Map(),
    };
    current.lastTimestampMs = Math.max(current.lastTimestampMs, timestampMs);
    this.states.set(vehicleId, current);
    return current;
  }

  add(event: TelemetryEvent): VehicleState {
    const timestampMs = Date.parse(event.timestamp);
    const state = this.getOrCreate(event.vehicleId, timestampMs);
    state.events.push(event);
    state.events.sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
    const cutoff = state.lastTimestampMs - this.maxAgeMs;
    state.events = state.events
      .filter((item) => Date.parse(item.timestamp) >= cutoff)
      .slice(-this.maxEvents);
    return state;
  }

  size(): number {
    return this.states.size;
  }
}
