import type { Logger } from 'pino';
import { parseTelemetryEvent, type TelemetryEvent } from '@fleetpulse/schemas';
import type { StreamProcessorConfig } from './config.js';
import { AlertManager } from './alerts.js';
import { createDetectors } from './rules.js';
import { BoundedVehicleStateStore } from './state.js';

export type ProcessorMetrics = {
  streamEventsConsumed: number;
  streamEventsValid: number;
  streamEventsInvalid: number;
  streamEventsTooLate: number;
  streamProcessingErrors: number;
};

export class StreamEventProcessor {
  readonly metrics: ProcessorMetrics = {
    streamEventsConsumed: 0,
    streamEventsValid: 0,
    streamEventsInvalid: 0,
    streamEventsTooLate: 0,
    streamProcessingErrors: 0,
  };
  private readonly state: BoundedVehicleStateStore;
  private readonly detectors;

  constructor(
    private readonly config: StreamProcessorConfig,
    private readonly alerts: AlertManager,
    private readonly logger: Logger<never, boolean>,
  ) {
    this.state = new BoundedVehicleStateStore(
      120,
      Math.max(config.stateTtlSeconds * 1000, 900_000),
    );
    this.detectors = createDetectors(config);
  }

  async process(input: unknown): Promise<void> {
    this.metrics.streamEventsConsumed += 1;
    let event: TelemetryEvent;
    try {
      event = parseTelemetryEvent(input);
    } catch (error) {
      this.metrics.streamEventsInvalid += 1;
      this.logger.warn({ err: error }, 'invalid stream telemetry rejected');
      return;
    }
    this.metrics.streamEventsValid += 1;
    const timestampMs = Date.parse(event.timestamp);
    const existing = this.state.get(event.vehicleId);
    if (existing && timestampMs < existing.lastTimestampMs - this.config.maxOutOfOrderMs) {
      this.metrics.streamEventsTooLate += 1;
      this.logger.debug(
        { eventId: event.eventId, vehicleId: event.vehicleId },
        'valid telemetry outside bounded lateness window',
      );
      return;
    }
    const state = this.state.add(event);
    const detections = this.detectors.flatMap((detector) => detector.evaluate(event, state));
    try {
      await this.alerts.handle(event, detections);
      if (detections.length === 0) {
        const count = (state.normalCounts.get('normal') ?? 0) + 1;
        state.normalCounts.set('normal', count);
        if (count >= this.config.normalEventsForResolution) {
          await this.alerts.resolveNormal(event, [
            'ENGINE_OVERHEATING',
            'RAPID_TEMPERATURE_RISE',
            'HARSH_BRAKING',
            'EXCESSIVE_IDLING',
            'LOW_BATTERY',
            'BATTERY_SOH_RISK',
            'CRITICAL_FAULT',
          ]);
          state.normalCounts.set('normal', 0);
        }
      } else state.normalCounts.set('normal', 0);
    } catch (error) {
      this.metrics.streamProcessingErrors += 1;
      this.logger.error({ err: error, eventId: event.eventId }, 'stream processing failed');
      throw error;
    }
  }
}
