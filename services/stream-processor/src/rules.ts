import type { TelemetryEvent } from '@fleetpulse/schemas';
import type { StreamProcessorConfig } from './config.js';
import type { VehicleState } from './state.js';

export type Detection = {
  type: string;
  severity: 'INFO' | 'WARNING' | 'CRITICAL';
  metadata: Record<string, unknown>;
  resolved?: boolean;
};

export type Detector = {
  name: string;
  evaluate: (event: TelemetryEvent, state: VehicleState) => Detection[];
};

function previousEvent(state: VehicleState, event: TelemetryEvent): TelemetryEvent | undefined {
  const index = state.events.lastIndexOf(event);
  return index > 0 ? state.events[index - 1] : undefined;
}

export function createDetectors(config: StreamProcessorConfig): Detector[] {
  return [
    {
      name: 'ENGINE_OVERHEATING',
      evaluate: (event, state) => {
        const temperature = event.engine?.temperatureC;
        if (temperature === undefined) return [];
        const since = Date.parse(event.timestamp) - config.engineTempWindowSeconds * 1000;
        const sustained =
          state.events.filter(
            (e) =>
              Date.parse(e.timestamp) >= since &&
              (e.engine?.temperatureC ?? 0) > config.engineTempThresholdC,
          ).length >= 2;
        return temperature > config.engineTempThresholdC && sustained
          ? [
              {
                type: 'ENGINE_OVERHEATING',
                severity: 'CRITICAL',
                metadata: {
                  rule: 'ENGINE_OVERHEATING',
                  observedTemperatureC: temperature,
                  thresholdC: config.engineTempThresholdC,
                  windowSeconds: config.engineTempWindowSeconds,
                },
              },
            ]
          : [];
      },
    },
    {
      name: 'RAPID_TEMPERATURE_RISE',
      evaluate: (event, state) => {
        const previous = previousEvent(state, event);
        const currentTemp = event.engine?.temperatureC;
        const previousTemp = previous?.engine?.temperatureC;
        if (!previous || currentTemp === undefined || previousTemp === undefined) return [];
        const seconds = Math.max(
          (Date.parse(event.timestamp) - Date.parse(previous.timestamp)) / 1000,
          0.001,
        );
        const rate = (currentTemp - previousTemp) / seconds;
        return rate > config.rapidTempRiseThreshold
          ? [
              {
                type: 'RAPID_TEMPERATURE_RISE',
                severity: 'WARNING',
                metadata: {
                  rule: 'RAPID_TEMPERATURE_RISE',
                  rateCPerSecond: rate,
                  thresholdCPerSecond: config.rapidTempRiseThreshold,
                },
              },
            ]
          : [];
      },
    },
    {
      name: 'HARSH_BRAKING',
      evaluate: (event, state) => {
        const previous = previousEvent(state, event);
        if (!previous) return [];
        const seconds = Math.max(
          (Date.parse(event.timestamp) - Date.parse(previous.timestamp)) / 1000,
          0.001,
        );
        const deceleration = (previous.motion.speedKph - event.motion.speedKph) / seconds;
        return deceleration > config.harshBrakingThreshold
          ? [
              {
                type: 'HARSH_BRAKING',
                severity: 'WARNING',
                metadata: {
                  rule: 'HARSH_BRAKING',
                  decelerationKphPerSecond: deceleration,
                  thresholdKphPerSecond: config.harshBrakingThreshold,
                },
              },
            ]
          : [];
      },
    },
    {
      name: 'EXCESSIVE_IDLING',
      evaluate: (event, state) => {
        if (!event.ignitionOn || event.motion.speedKph > 1) return [];
        const since = Date.parse(event.timestamp) - config.idleThresholdSeconds * 1000;
        const idle = state.events.filter(
          (e) =>
            e.ignitionOn &&
            e.motion.speedKph <= 1 &&
            Date.parse(e.timestamp) <= Date.parse(event.timestamp) &&
            Date.parse(e.timestamp) >= since,
        );
        return idle.length >= 2 &&
          Date.parse(idle[idle.length - 1].timestamp) - Date.parse(idle[0].timestamp) >=
            config.idleThresholdSeconds * 1000
          ? [
              {
                type: 'EXCESSIVE_IDLING',
                severity: 'INFO',
                metadata: {
                  rule: 'EXCESSIVE_IDLING',
                  durationSeconds: config.idleThresholdSeconds,
                },
              },
            ]
          : [];
      },
    },
    {
      name: 'LOW_BATTERY',
      evaluate: (event) =>
        event.battery && event.battery.socPercent < config.lowBatteryThresholdPercent
          ? [
              {
                type: 'LOW_BATTERY',
                severity: 'WARNING',
                metadata: {
                  rule: 'LOW_BATTERY',
                  observedSocPercent: event.battery.socPercent,
                  thresholdPercent: config.lowBatteryThresholdPercent,
                },
              },
            ]
          : [],
    },
    {
      name: 'BATTERY_SOH_RISK',
      evaluate: (event) =>
        event.battery && event.battery.sohPercent < config.lowBatterySohThresholdPercent
          ? [
              {
                type: 'BATTERY_SOH_RISK',
                severity: 'WARNING',
                metadata: {
                  rule: 'BATTERY_SOH_RISK',
                  observedSohPercent: event.battery.sohPercent,
                  thresholdPercent: config.lowBatterySohThresholdPercent,
                },
              },
            ]
          : [],
    },
    {
      name: 'CRITICAL_FAULT',
      evaluate: (event) =>
        event.eventType === 'FAULT' && event.fault?.severity === 'CRITICAL'
          ? [
              {
                type: 'CRITICAL_FAULT',
                severity: 'CRITICAL',
                metadata: {
                  rule: 'CRITICAL_FAULT',
                  scenario: event.fault.scenario,
                  signal: event.fault.signal,
                },
              },
            ]
          : [],
    },
  ];
}
