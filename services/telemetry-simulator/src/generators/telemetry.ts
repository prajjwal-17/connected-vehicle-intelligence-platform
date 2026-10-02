import { randomUUID } from 'node:crypto';
import { telemetryEventSchema, type TelemetryEvent } from '@fleetpulse/schemas';
import { SeededRandom } from '../models/random.js';
import type { VirtualVehicle } from '../models/vehicle.js';

function normalizeHeading(value: number): number {
  return (value + 360) % 360;
}

function clampTemperature(value: number): number {
  return Math.min(180, Math.max(-40, value));
}

function moveVehicle(vehicle: VirtualVehicle, deltaSeconds: number) {
  const distanceKm = vehicle.speedKph * (deltaSeconds / 3_600);
  const latitudeDelta = (distanceKm * Math.cos((vehicle.heading * Math.PI) / 180)) / 111;
  const longitudeScale = Math.max(0.2, Math.cos((vehicle.latitude * Math.PI) / 180));
  const longitudeDelta =
    (distanceKm * Math.sin((vehicle.heading * Math.PI) / 180)) / (111 * longitudeScale);
  vehicle.latitude += latitudeDelta;
  vehicle.longitude += longitudeDelta;
  vehicle.odometerKm += distanceKm;
}

export function advanceVehicle(
  vehicle: VirtualVehicle,
  random: SeededRandom,
  deltaSeconds: number,
) {
  const previousState = vehicle.tripState;
  const previousIgnitionState = vehicle.ignitionState;
  vehicle.lastEventTimestamp += deltaSeconds * 1_000;

  if (vehicle.faultScenario) {
    vehicle.faultAgeSeconds += deltaSeconds;
  }

  if (vehicle.tripState === 'MAINTENANCE') {
    vehicle.speedKph = 0;
    vehicle.ignitionState = false;
  } else if (vehicle.tripState === 'CHARGING') {
    vehicle.speedKph = 0;
    vehicle.ignitionState = false;
    if (vehicle.batterySocPercent !== null) {
      vehicle.batterySocPercent = Math.min(100, vehicle.batterySocPercent + deltaSeconds * 0.35);
    }
    if (random.next() < 0.03) {
      vehicle.tripState = 'PARKED';
      vehicle.pendingEventType = 'CHARGING_ENDED';
    }
  } else if (vehicle.tripState === 'DRIVING') {
    vehicle.ignitionState = true;
    vehicle.speedKph = Math.max(0, Math.min(130, vehicle.speedKph + random.between(-4, 4)));
    vehicle.heading = normalizeHeading(vehicle.heading + random.between(-8, 8));
    moveVehicle(vehicle, deltaSeconds);
    if (vehicle.powertrainType !== 'EV') {
      vehicle.engineRpm = Math.max(700, vehicle.speedKph * random.between(35, 55));
      vehicle.engineTemperatureC += random.between(-0.7, 0.9);
      if (vehicle.fuelLevelPercent !== null)
        vehicle.fuelLevelPercent = Math.max(0, vehicle.fuelLevelPercent - deltaSeconds * 0.002);
    } else if (vehicle.batterySocPercent !== null) {
      vehicle.batterySocPercent = Math.max(0, vehicle.batterySocPercent - deltaSeconds * 0.025);
    }
    if (random.next() < 0.02) {
      vehicle.tripState = 'IDLE';
      vehicle.speedKph = 0;
    }
  } else {
    vehicle.speedKph = 0;
    vehicle.ignitionState = vehicle.tripState === 'IDLE';
    if (vehicle.powertrainType !== 'EV') {
      vehicle.engineRpm = vehicle.tripState === 'IDLE' ? random.between(650, 900) : 0;
      vehicle.engineTemperatureC += random.between(-0.4, 0.4);
    }
    if (random.next() < 0.05) {
      vehicle.tripState = 'DRIVING';
      vehicle.ignitionState = true;
      vehicle.tripId = randomUUID();
      vehicle.speedKph = random.between(15, 55);
    } else if (vehicle.tripState === 'IDLE' && random.next() < 0.03) {
      vehicle.tripState = 'PARKED';
      vehicle.ignitionState = false;
    } else if (vehicle.powertrainType !== 'ICE' && random.next() < 0.01) {
      vehicle.tripState = 'CHARGING';
      vehicle.pendingEventType = 'CHARGING_STARTED';
    }
  }

  if (previousState !== vehicle.tripState && vehicle.pendingEventType === null) {
    if (vehicle.tripState === 'DRIVING') vehicle.pendingEventType = 'TRIP_STARTED';
    else if (previousState === 'DRIVING') vehicle.pendingEventType = 'TRIP_ENDED';
  }
  if (vehicle.pendingEventType === null && previousIgnitionState !== vehicle.ignitionState) {
    vehicle.pendingEventType = vehicle.ignitionState ? 'IGNITION_ON' : 'IGNITION_OFF';
  }

  applyFaultSignals(vehicle);
}

function applyFaultSignals(vehicle: VirtualVehicle) {
  if (!vehicle.faultScenario || vehicle.faultAgeSeconds < 3) return;

  if (vehicle.faultScenario === 'ENGINE_OVERHEATING' && vehicle.powertrainType !== 'EV') {
    vehicle.engineTemperatureC += Math.min(55, vehicle.faultAgeSeconds * 0.25);
  }
  if (vehicle.faultScenario === 'BATTERY_DEGRADATION' && vehicle.batterySohPercent !== null) {
    vehicle.batterySohPercent = Math.max(
      65,
      vehicle.batterySohPercent - 0.01 * vehicle.faultAgeSeconds,
    );
    if (vehicle.batterySocPercent !== null)
      vehicle.batterySocPercent = Math.max(
        0,
        vehicle.batterySocPercent - 0.015 * vehicle.faultAgeSeconds,
      );
  }

  if (vehicle.faultAgeSeconds >= 8 && !vehicle.faultEmitted) {
    vehicle.faultEmitted = true;
    vehicle.pendingEventType = 'FAULT';
  } else if (vehicle.faultAgeSeconds >= 12 && !vehicle.maintenanceDueEmitted) {
    vehicle.maintenanceDueEmitted = true;
    vehicle.pendingEventType = 'MAINTENANCE_DUE';
  }
}

function faultPayload(vehicle: VirtualVehicle): TelemetryEvent['fault'] {
  if (!vehicle.faultScenario) return undefined;
  const signal =
    vehicle.faultScenario === 'ENGINE_OVERHEATING'
      ? 'engine temperature rising'
      : vehicle.faultScenario === 'BATTERY_DEGRADATION'
        ? 'battery state of health declining'
        : vehicle.faultScenario === 'BRAKE_DEGRADATION'
          ? 'braking distance proxy increasing'
          : 'tire pressure anomaly';
  return {
    scenario: vehicle.faultScenario,
    signal,
    severity: vehicle.faultEmitted ? 'CRITICAL' : 'WARNING',
  };
}

export type TelemetryGenerationTimings = {
  generationMs: number;
  schemaValidationMs: number;
};

export function generateTelemetryEvent(
  vehicle: VirtualVehicle,
  timestamp = new Date(vehicle.lastEventTimestamp),
  timings?: TelemetryGenerationTimings,
): TelemetryEvent {
  const generationStarted = performance.now();
  vehicle.sequenceNumber += 1;
  const eventType = vehicle.pendingEventType ?? 'TELEMETRY';
  vehicle.pendingEventType = null;
  const event: TelemetryEvent = {
    schemaVersion: '1.0',
    eventId: randomUUID(),
    vehicleId: vehicle.vehicleId,
    vin: vehicle.vin,
    timestamp: timestamp.toISOString(),
    sequenceNumber: vehicle.sequenceNumber,
    eventType,
    vehicleState: vehicle.tripState,
    location: { latitude: vehicle.latitude, longitude: vehicle.longitude },
    motion: { speedKph: vehicle.speedKph, heading: normalizeHeading(vehicle.heading) },
    ignitionOn: vehicle.ignitionState,
    tripId: vehicle.tripId,
    engine:
      vehicle.powertrainType === 'EV'
        ? undefined
        : { rpm: vehicle.engineRpm, temperatureC: clampTemperature(vehicle.engineTemperatureC) },
    fuel:
      vehicle.fuelLevelPercent === null ? undefined : { levelPercent: vehicle.fuelLevelPercent },
    battery:
      vehicle.batterySocPercent === null || vehicle.batterySohPercent === null
        ? undefined
        : { socPercent: vehicle.batterySocPercent, sohPercent: vehicle.batterySohPercent },
    fault:
      eventType === 'FAULT' || eventType === 'MAINTENANCE_DUE' ? faultPayload(vehicle) : undefined,
  };
  if (timings) timings.generationMs += performance.now() - generationStarted;
  const validationStarted = performance.now();
  const validated = telemetryEventSchema.parse(event);
  if (timings) timings.schemaValidationMs += performance.now() - validationStarted;
  return validated;
}
