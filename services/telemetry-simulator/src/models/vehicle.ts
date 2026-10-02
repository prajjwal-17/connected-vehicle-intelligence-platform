import { randomUUID } from 'node:crypto';
import type { VehicleState } from '@fleetpulse/schemas';
import { SeededRandom } from './random.js';

export type PowertrainType = 'ICE' | 'HYBRID' | 'EV';
export type FaultScenario =
  'ENGINE_OVERHEATING' | 'BATTERY_DEGRADATION' | 'BRAKE_DEGRADATION' | 'TIRE_PRESSURE';

export type RegionBounds = {
  minLatitude: number;
  maxLatitude: number;
  minLongitude: number;
  maxLongitude: number;
};

export const regions: Record<string, RegionBounds> = {
  'north-india': {
    minLatitude: 28.35,
    maxLatitude: 28.85,
    minLongitude: 76.75,
    maxLongitude: 77.55,
  },
  'us-west': {
    minLatitude: 37.15,
    maxLatitude: 38.05,
    minLongitude: -122.65,
    maxLongitude: -121.75,
  },
  europe: { minLatitude: 48.65, maxLatitude: 49.15, minLongitude: 2.15, maxLongitude: 2.65 },
};

const oems = ['Toyota', 'Ford', 'Hyundai', 'Tata', 'Volkswagen', 'Tesla'] as const;
const models = ['Atlas', 'Transit', 'Nexo', 'Ace', 'ID.4', 'Model Y'] as const;
const powertrains: PowertrainType[] = ['ICE', 'HYBRID', 'EV'];
const initialStates: VehicleState[] = ['PARKED', 'DRIVING', 'IDLE', 'CHARGING', 'MAINTENANCE'];

export type VirtualVehicle = {
  vehicleId: string;
  vin: string;
  oem: string;
  model: string;
  modelYear: number;
  powertrainType: PowertrainType;
  latitude: number;
  longitude: number;
  speedKph: number;
  heading: number;
  odometerKm: number;
  engineRpm: number;
  engineTemperatureC: number;
  fuelLevelPercent: number | null;
  batterySocPercent: number | null;
  batterySohPercent: number | null;
  ignitionState: boolean;
  tripState: VehicleState;
  tripId: string | null;
  lastEventTimestamp: number;
  sequenceNumber: number;
  faultScenario: FaultScenario | null;
  faultAgeSeconds: number;
  faultEmitted: boolean;
  maintenanceDueEmitted: boolean;
  pendingEventType: VehicleEventType | null;
};

export type VehicleEventType =
  | 'TRIP_STARTED'
  | 'TRIP_ENDED'
  | 'IGNITION_ON'
  | 'IGNITION_OFF'
  | 'CHARGING_STARTED'
  | 'CHARGING_ENDED'
  | 'FAULT'
  | 'MAINTENANCE_DUE';

export function generateVin(index: number): string {
  return `FP${String(index).padStart(15, '0')}`;
}

export function createVehicle(
  index: number,
  random: SeededRandom,
  region: RegionBounds,
): VirtualVehicle {
  const powertrainType = random.pick(powertrains);
  const tripState = random.pick(initialStates);
  const isElectric = powertrainType === 'EV';
  const faultScenario =
    index % 1_000 === 0
      ? 'ENGINE_OVERHEATING'
      : index % 1_000 === 1
        ? 'BATTERY_DEGRADATION'
        : index % 1_000 === 2
          ? 'BRAKE_DEGRADATION'
          : index % 1_000 === 3
            ? 'TIRE_PRESSURE'
            : null;

  return {
    vehicleId: `vehicle-${index + 1}`,
    vin: generateVin(index + 1),
    oem: random.pick(oems),
    model: random.pick(models),
    modelYear: random.integer(2018, 2026),
    powertrainType,
    latitude: random.between(region.minLatitude, region.maxLatitude),
    longitude: random.between(region.minLongitude, region.maxLongitude),
    speedKph: tripState === 'DRIVING' ? random.between(20, 85) : 0,
    heading: random.between(0, 360),
    odometerKm: random.between(5_000, 180_000),
    engineRpm: isElectric ? 0 : tripState === 'DRIVING' ? random.between(900, 2_800) : 0,
    engineTemperatureC: isElectric ? 28 : random.between(78, 96),
    fuelLevelPercent: isElectric ? null : random.between(20, 95),
    batterySocPercent: isElectric || powertrainType === 'HYBRID' ? random.between(35, 95) : null,
    batterySohPercent: isElectric || powertrainType === 'HYBRID' ? random.between(88, 100) : null,
    ignitionState: tripState === 'DRIVING' || tripState === 'IDLE',
    tripState,
    tripId: tripState === 'DRIVING' ? randomUUID() : null,
    lastEventTimestamp: Date.now(),
    sequenceNumber: 0,
    faultScenario,
    faultAgeSeconds: 0,
    faultEmitted: false,
    maintenanceDueEmitted: false,
    pendingEventType: null,
  };
}

export function createVehiclePopulation(
  count: number,
  seed: number,
  regionName: string,
  indexOffset = 0,
): VirtualVehicle[] {
  const region = regions[regionName];
  if (!region) throw new Error(`Unknown simulator region: ${regionName}`);
  const random = new SeededRandom(seed);
  const vehicles = new Array<VirtualVehicle>(count);
  for (let index = 0; index < count; index += 1) {
    vehicles[index] = createVehicle(index + indexOffset, random, region);
  }
  return vehicles;
}
