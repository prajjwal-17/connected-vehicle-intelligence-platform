import type { Logger } from 'pino';
import { parseTelemetryEvent, type TelemetryEvent } from '@fleetpulse/schemas';
import type { StreamProcessorConfig } from './config.js';
import { AlertManager } from './alerts.js';
import { createDetectors } from './rules.js';
import { BoundedVehicleStateStore } from './state.js';

export type ProcessorMetrics = {
  eventsProcessed: number;
  streamEventsConsumed: number;
  streamEventsValid: number;
  streamEventsInvalid: number;
  streamEventsTooLate: number;
  streamProcessingErrors: number;
  schemaValidationMs: number;
  stateLookupMs: number;
  detectorEvaluationMs: number;
  detectorMs: Record<string, number>;
  totalProcessingMs: number;
};

export class StreamEventProcessor {
  readonly metrics: ProcessorMetrics = {
    eventsProcessed: 0,
    streamEventsConsumed: 0,
    streamEventsValid: 0,
    streamEventsInvalid: 0,
    streamEventsTooLate: 0,
    streamProcessingErrors: 0,
    schemaValidationMs: 0,
    stateLookupMs: 0,
    detectorEvaluationMs: 0,
    detectorMs: {},
    totalProcessingMs: 0,
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
    const event = this.parseEvent(input);
    if (!event) return;
    await this.processParsed(event);
  }

  async processBatch(
    inputs: unknown[],
    onProcessed?: (index: number) => void | Promise<void>,
    onFailed?: (index: number) => void | Promise<void>,
  ): Promise<void> {
    const events: Array<{ event: TelemetryEvent; inputIndex: number }> = [];
    const invalidInputIndexes: number[] = [];
    for (let index = 0; index < inputs.length; index += 1) {
      this.metrics.streamEventsConsumed += 1;
      const event = this.parseEvent(inputs[index]);
      if (event) events.push({ event, inputIndex: index });
      else invalidInputIndexes.push(index);
    }
    await this.alerts.prefetchVehicleLookups(events.map(({ event }) => event));
    const validEvents = new Map(events.map(({ event, inputIndex }) => [inputIndex, event]));
    for (let index = 0; index < inputs.length; index += 1) {
      const event = validEvents.get(index);
      if (!event) {
        if (invalidInputIndexes.includes(index)) await onProcessed?.(index);
        continue;
      }
      try {
        await this.processParsed(event);
      } catch (error) {
        await onFailed?.(index);
        throw error;
      }
      await onProcessed?.(index);
    }
  }

  private parseEvent(input: unknown): TelemetryEvent | undefined {
    const validationStarted = performance.now();
    try {
      const event = parseTelemetryEvent(input);
      this.metrics.schemaValidationMs += performance.now() - validationStarted;
      this.metrics.streamEventsValid += 1;
      return event;
    } catch (error) {
      this.metrics.schemaValidationMs += performance.now() - validationStarted;
      this.metrics.streamEventsInvalid += 1;
      this.logger.warn({ err: error }, 'invalid stream telemetry rejected');
      return undefined;
    }
  }

  private async processParsed(event: TelemetryEvent): Promise<void> {
    const totalStarted = performance.now();
    const timestampMs = Date.parse(event.timestamp);
    const stateStarted = performance.now();
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
    this.metrics.stateLookupMs += performance.now() - stateStarted;
    const detectorStarted = performance.now();
    const detections = this.detectors.flatMap((detector) => {
      const started = performance.now();
      const result = detector.evaluate(event, state);
      const elapsed = performance.now() - started;
      this.metrics.detectorMs[detector.name] =
        (this.metrics.detectorMs[detector.name] ?? 0) + elapsed;
      return result;
    });
    this.metrics.detectorEvaluationMs += performance.now() - detectorStarted;
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
      this.metrics.eventsProcessed += 1;
      this.metrics.totalProcessingMs += performance.now() - totalStarted;
    } catch (error) {
      this.metrics.streamProcessingErrors += 1;
      this.logger.error({ err: error, eventId: event.eventId }, 'stream processing failed');
      throw error;
    }
  }
}
