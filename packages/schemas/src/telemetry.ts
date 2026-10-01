import { z } from 'zod';

export const telemetryEventTypes = [
  'TELEMETRY',
  'TRIP_STARTED',
  'TRIP_ENDED',
  'IGNITION_ON',
  'IGNITION_OFF',
  'CHARGING_STARTED',
  'CHARGING_ENDED',
  'FAULT',
  'MAINTENANCE_DUE',
] as const;

export const vehicleStates = ['PARKED', 'DRIVING', 'IDLE', 'CHARGING', 'MAINTENANCE'] as const;

const locationSchema = z.object({
  latitude: z.number().min(-90).max(90),
  longitude: z.number().min(-180).max(180),
});

const motionSchema = z.object({
  speedKph: z.number().min(0).max(240),
  heading: z.number().min(0).lt(360),
});

const engineSchema = z.object({
  rpm: z.number().min(0).max(10000),
  temperatureC: z.number().min(-40).max(180),
});

const fuelSchema = z.object({ levelPercent: z.number().min(0).max(100) });

const batterySchema = z.object({
  socPercent: z.number().min(0).max(100),
  sohPercent: z.number().min(0).max(100),
});

const faultSchema = z.object({
  scenario: z.enum([
    'ENGINE_OVERHEATING',
    'BATTERY_DEGRADATION',
    'BRAKE_DEGRADATION',
    'TIRE_PRESSURE',
  ]),
  signal: z.string().min(1),
  severity: z.enum(['WARNING', 'CRITICAL']),
});

export const telemetryEventSchema = z.object({
  schemaVersion: z.literal('1.0'),
  eventId: z.string().uuid(),
  vehicleId: z.string().min(1),
  vin: z.string().length(17),
  timestamp: z.string().datetime({ offset: true }),
  deliveryTimestamp: z.string().datetime({ offset: true }).optional(),
  sequenceNumber: z.number().int().nonnegative(),
  eventType: z.enum(telemetryEventTypes),
  vehicleState: z.enum(vehicleStates),
  location: locationSchema,
  motion: motionSchema,
  ignitionOn: z.boolean(),
  tripId: z.string().uuid().nullable(),
  engine: engineSchema.optional(),
  fuel: fuelSchema.optional(),
  battery: batterySchema.optional(),
  fault: faultSchema.optional(),
});

export type TelemetryEvent = z.infer<typeof telemetryEventSchema>;
export type TelemetryEventType = (typeof telemetryEventTypes)[number];
export type VehicleState = (typeof vehicleStates)[number];

export function parseTelemetryEvent(input: unknown): TelemetryEvent {
  return telemetryEventSchema.parse(input);
}
