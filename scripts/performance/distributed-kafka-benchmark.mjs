/* global clearInterval, clearTimeout, console, performance, process, setInterval, setTimeout */

import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';

const execFileAsync = promisify(execFile);
const root = process.cwd();
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
const events = Number(args.get('--events') ?? 100_000);
const producerWorkers = Number(args.get('--producer-workers') ?? 1);
const consumerWorkers = Number(args.get('--consumer-workers') ?? producerWorkers);
const partitions = Number(args.get('--partitions') ?? 6);
const batchSize = Number(args.get('--batch-size') ?? 500);
const batchConcurrency = Number(args.get('--batch-concurrency') ?? 1);
const maxInFlightRequests = Number(args.get('--max-in-flight-requests') ?? 5);
const downstreamMode = args.get('--downstream-mode') ?? 'none';
const downstreamRoles = (args.get('--downstream-roles') ?? 'stream,historical')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
if (!['none', 'noop', 'real'].includes(downstreamMode))
  throw new Error('--downstream-mode must be none, noop, or real');
if (
  ![events, producerWorkers, consumerWorkers, partitions].every(Number.isInteger) ||
  events < 1 ||
  producerWorkers < 1 ||
  consumerWorkers < 0 ||
  partitions < 1
)
  throw new Error('events and worker/partition counts must be positive integers');

const topic = args.get('--topic') ?? `fleetpulse.benchmark.${Date.now()}`;
const groupId = `fleetpulse-benchmark-${randomUUID()}`;
const commonEnv = {
  ...process.env,
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_TOPIC: topic,
  KAFKA_PARTITIONS: String(partitions),
  KAFKA_REPLICATION_FACTOR: '1',
  KAFKA_BATCH_SIZE: String(batchSize),
  KAFKA_BATCH_MAX_BYTES: '1000000',
  KAFKA_BATCH_CONCURRENCY: String(batchConcurrency),
  KAFKA_MAX_IN_FLIGHT_REQUESTS: String(maxInFlightRequests),
  KAFKA_COMPRESSION: 'true',
  KAFKA_GROUP_ID: groupId,
  KAFKA_FROM_BEGINNING: 'false',
  REDIS_URL: 'redis://localhost:6379',
  IDEMPOTENCY_TTL_SECONDS: '3600',
  LOG_LEVEL: 'warn',
};

const children = [];
const downstreamChildren = [];
const output = [];
function start(command, commandArgs, env) {
  const child = spawn(command, commandArgs, {
    cwd: root,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
  child.lines = [];
  child.stdout.on('data', (chunk) => {
    const lines = String(chunk).split(/\r?\n/).filter(Boolean);
    child.lines.push(...lines);
    output.push(...lines);
  });
  child.stderr.on('data', (chunk) => output.push(...String(chunk).split(/\r?\n/).filter(Boolean)));
  children.push(child);
  return child;
}
function waitFor(child, predicate, timeoutMs = 30_000) {
  return new Promise((resolve, reject) => {
    if (
      child.lines.some((line) => {
        try {
          return predicate(JSON.parse(line));
        } catch {
          return false;
        }
      })
    ) {
      resolve();
      return;
    }
    const timer = setTimeout(() => reject(new Error('worker startup timed out')), timeoutMs);
    const onData = (chunk) => {
      for (const line of String(chunk).split(/\r?\n/)) {
        try {
          if (predicate(JSON.parse(line))) {
            clearTimeout(timer);
            resolve();
          }
        } catch {
          // Ignore non-JSON startup log lines.
        }
      }
    };
    child.stdout.on('data', onData);
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`worker exited during startup: ${code}`));
    });
  });
}
async function resources() {
  try {
    const { stdout } = await execFileAsync('docker', [
      'stats',
      '--no-stream',
      '--format',
      '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}',
    ]);
    return stdout.trim().split(/\r?\n/).filter(Boolean);
  } catch {
    return [];
  }
}
async function stopAll() {
  for (const child of children) if (!child.killed) child.kill('SIGTERM');
}

const kafka = new Kafka({
  clientId: `fleetpulse-benchmark-${randomUUID()}`,
  brokers: ['localhost:9092'],
});
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({
  waitForLeaders: true,
  topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
});
const beforeResources = await resources();
const consumers = [];
const resourceSamples = [];
let resourceSampler;
try {
  const downstreamGroups = [];
  if (downstreamMode !== 'none') {
    const suffix = randomUUID();
    const downstreamSpecs = [
      ['stream', `fleetpulse-isolation-stream-${suffix}`],
      ['historical', `fleetpulse-isolation-historical-${suffix}`],
    ];
    for (const [role, groupId] of downstreamSpecs.filter(([role]) =>
      downstreamRoles.includes(role),
    )) {
      downstreamGroups.push({ role, groupId });
      const env = {
        ...commonEnv,
        KAFKA_GROUP_ID: groupId,
        DOWNSTREAM_ROLE: role,
        ...(downstreamMode === 'real'
          ? role === 'stream'
            ? { STREAM_PROCESSOR_GROUP: groupId, STREAM_PROFILE: '1', LOG_LEVEL: 'warn' }
            : { HISTORICAL_ANALYTICS_GROUP: groupId, LOG_LEVEL: 'warn' }
          : {}),
      };
      const child =
        downstreamMode === 'noop'
          ? start(process.execPath, ['scripts/performance/downstream-isolation-worker.mjs'], env)
          : start(
              process.execPath,
              [
                'node_modules/tsx/dist/cli.mjs',
                role === 'stream'
                  ? 'services/stream-processor/src/main.ts'
                  : 'services/historical-analytics/src/main.ts',
              ],
              env,
            );
      downstreamChildren.push({ role, groupId, child });
      if (downstreamMode === 'noop') await waitFor(child, (value) => value.ready === true);
      else await new Promise((resolve) => setTimeout(resolve, 2_000));
    }
  }
  for (let index = 0; index < consumerWorkers; index += 1) {
    const child = start(
      process.execPath,
      ['node_modules/tsx/dist/cli.mjs', 'scripts/performance/distributed-consumer-worker.ts'],
      {
        ...commonEnv,
        BENCHMARK_WORKER_ID: String(index),
        KAFKA_CLIENT_ID: `fleetpulse-benchmark-consumer-${index}`,
      },
    );
    consumers.push(child);
    await waitFor(child, (value) => value.ready === true);
    console.error(`consumer ${index + 1}/${consumerWorkers} ready`);
  }
  await new Promise((resolve) => setTimeout(resolve, 2_000));

  const startedAt = performance.now();
  resourceSampler = setInterval(() => {
    void resources().then((sample) =>
      resourceSamples.push({ elapsedMs: performance.now() - startedAt, sample }),
    );
  }, 1_000);
  const perWorker = Math.floor(events / producerWorkers);
  const remainder = events % producerWorkers;
  const producers = [];
  for (let index = 0; index < producerWorkers; index += 1) {
    const count = perWorker + (index < remainder ? 1 : 0);
    producers.push(
      start(
        process.execPath,
        [
          'node_modules/tsx/dist/cli.mjs',
          'services/telemetry-simulator/src/index.ts',
          '--vehicles',
          String(Math.max(100, Math.ceil(count / 10))),
          '--max-events',
          String(count),
          '--vehicle-index-offset',
          String(index * 1_000_000),
          '--output-mode',
          'kafka',
          '--seed',
          String(42 + index),
        ],
        {
          ...commonEnv,
          LOG_LEVEL: 'info',
          PRODUCER_PROFILE: '1',
          BENCHMARK_WORKER_ID: String(index),
          KAFKA_CLIENT_ID: `fleetpulse-benchmark-producer-${index}`,
        },
      ),
    );
  }
  const producerResults = await Promise.all(
    producers.map(
      (child) => new Promise((resolve) => child.once('exit', (code) => resolve(code ?? 1))),
    ),
  );
  clearInterval(resourceSampler);
  console.error('producers exited');
  const producerWallDurationMs = performance.now() - startedAt;
  const actualProduced = output
    .filter((line) => line.includes('"msg":"simulator stopped"'))
    .reduce((sum, line) => {
      try {
        return sum + JSON.parse(line).eventsGenerated;
      } catch {
        return sum;
      }
    }, 0);
  const deadline = Date.now() + 120_000;
  let lag = Number.POSITIVE_INFINITY;
  let downstreamLagSnapshot = {};
  const lagSamples = [];
  while (consumerWorkers > 0 && Date.now() < deadline) {
    const topicOffsets = await admin.fetchTopicOffsets(topic);
    const groupOffsetTopics = await admin.fetchOffsets({ groupId, topics: [topic] });
    const groupOffsets = groupOffsetTopics.flatMap((item) => item.partitions);
    lag = topicOffsets.reduce((sum, partition) => {
      const committed =
        groupOffsets.find((item) => item.partition === partition.partition)?.offset ?? '0';
      return sum + Math.max(0, Number(partition.offset) - Number(committed));
    }, 0);
    downstreamLagSnapshot = {};
    for (const downstream of downstreamGroups) {
      const offsets = await admin.fetchOffsets({ groupId: downstream.groupId, topics: [topic] });
      const committed = offsets.flatMap((item) => item.partitions);
      downstreamLagSnapshot[downstream.role] = topicOffsets.reduce(
        (sum, partition) =>
          sum +
          Math.max(
            0,
            Number(partition.offset) -
              Number(committed.find((item) => item.partition === partition.partition)?.offset ?? 0),
          ),
        0,
      );
    }
    lagSamples.push({ elapsedMs: performance.now() - startedAt, lag });
    if (
      lag === 0 &&
      actualProduced === events &&
      Object.values(downstreamLagSnapshot).every((value) => value === 0)
    )
      break;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  if (consumerWorkers > 0) await new Promise((resolve) => setTimeout(resolve, 1_000));
  const durationMs = consumerWorkers === 0 ? producerWallDurationMs : performance.now() - startedAt;
  if (consumerWorkers === 0) lag = 0;
  const group = consumerWorkers > 0 ? await admin.describeGroups([groupId]) : undefined;
  const finalTopicOffsets = await admin.fetchTopicOffsets(topic);
  const groupLags = { ...downstreamLagSnapshot };
  for (const downstream of downstreamGroups) {
    if (groupLags[downstream.role] === 0) continue;
    const offsets = await admin.fetchOffsets({ groupId: downstream.groupId, topics: [topic] });
    const committed = offsets.flatMap((item) => item.partitions);
    groupLags[downstream.role] = finalTopicOffsets.reduce(
      (sum, partition) =>
        sum +
        Math.max(
          0,
          Number(partition.offset) -
            Number(committed.find((item) => item.partition === partition.partition)?.offset ?? 0),
        ),
      0,
    );
  }
  const afterResources = await resources();
  await stopAll();
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  const downstreamMetrics = Object.fromEntries(
    downstreamChildren.map(({ role, child }) => [
      role,
      child.lines
        .map((line) => {
          try {
            const parsed = JSON.parse(line);
            return parsed.metrics ?? parsed.streamProfile;
          } catch {
            return undefined;
          }
        })
        .filter(Boolean)
        .at(-1),
    ]),
  );
  const summaries = output
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
  const consumerMetrics = consumers
    .map((child) =>
      child.lines
        .map((line) => {
          try {
            return JSON.parse(line).metrics;
          } catch {
            return undefined;
          }
        })
        .filter(Boolean)
        .at(-1),
    )
    .filter(Boolean);
  const consumerProfileSamples = consumers.flatMap((child) =>
    child.lines
      .map((line) => {
        try {
          const value = JSON.parse(line);
          return value.profile ? value : undefined;
        } catch {
          return undefined;
        }
      })
      .filter(Boolean),
  );
  const consumerCpuByWorker = Object.groupBy(consumerProfileSamples, (sample) =>
    String(sample.profile.workerId ?? sample.pid ?? 'unknown'),
  );
  const consumerCpuWindows = Object.groupBy(consumerProfileSamples, (sample) =>
    String(Math.round(sample.time / 1_000)),
  );
  const aggregateConsumerCpuSamples = Object.values(consumerCpuWindows).map((samples) =>
    samples.reduce((sum, sample) => sum + sample.profile.cpuUtilizationPercent, 0),
  );
  const consumedProcessed = consumerMetrics.reduce(
    (sum, item) => sum + (item.telemetryEventsProcessed ?? 0),
    0,
  );
  const redisCommands = consumerMetrics.reduce((sum, item) => sum + (item.redisCommands ?? 0), 0);
  const producerMetrics = producers
    .map((child) =>
      child.lines
        .map((line) => {
          try {
            const value = JSON.parse(line);
            return value.msg === 'simulator stopped' ? value : undefined;
          } catch {
            return undefined;
          }
        })
        .filter(Boolean)
        .at(-1),
    )
    .filter(Boolean);
  const producerProfileSamples = producers.flatMap((child) =>
    child.lines
      .map((line) => {
        try {
          const value = JSON.parse(line);
          return value.msg === 'producer profile sample' ? value : undefined;
        } catch {
          return undefined;
        }
      })
      .filter(Boolean),
  );
  const workerCpuUtilization = Object.groupBy(producerProfileSamples, (sample) =>
    String(sample.workerId),
  );
  const perWorkerCpu = Object.fromEntries(
    Object.entries(workerCpuUtilization).map(([workerId, samples]) => [
      workerId,
      samples.reduce((sum, sample) => sum + sample.cpuUtilizationPercent, 0) / samples.length,
    ]),
  );
  const cpuWindows = Object.groupBy(producerProfileSamples, (sample) =>
    String(Math.round(sample.time / 1_000)),
  );
  const aggregateCpuSamples = Object.values(cpuWindows).map((samples) =>
    samples.reduce((sum, sample) => sum + sample.cpuUtilizationPercent, 0),
  );
  const profiling = producerMetrics.reduce(
    (sum, item) => ({
      eventGenerationMs: sum.eventGenerationMs + (item.profiling?.eventGenerationMs ?? 0),
      schemaValidationMs: sum.schemaValidationMs + (item.profiling?.schemaValidationMs ?? 0),
      serializationMs: sum.serializationMs + (item.profiling?.serializationMs ?? 0),
      kafkaSendWaitMs: sum.kafkaSendWaitMs + (item.profiling?.kafkaSendWaitMs ?? 0),
      batchesSent: sum.batchesSent + (item.profiling?.batchesSent ?? 0),
      successfulSends: sum.successfulSends + (item.profiling?.successfulSends ?? 0),
      failedSends: sum.failedSends + (item.profiling?.failedSends ?? 0),
      averageBatchSendLatencyMs:
        sum.averageBatchSendLatencyMs + (item.profiling?.averageBatchSendLatencyMs ?? 0),
      p95BatchSendLatencyMs: Math.max(
        sum.p95BatchSendLatencyMs,
        item.profiling?.p95BatchSendLatencyMs ?? 0,
      ),
      inFlightPeak: Math.max(sum.inFlightPeak, item.profiling?.inFlightPeak ?? 0),
      queueDepthPeak: Math.max(sum.queueDepthPeak, item.profiling?.queueDepthPeak ?? 0),
      cpuUserMs: sum.cpuUserMs + (item.cpuUserMs ?? 0),
      cpuSystemMs: sum.cpuSystemMs + (item.cpuSystemMs ?? 0),
      rssMb: Math.max(sum.rssMb, item.rssMb ?? 0),
    }),
    {
      eventGenerationMs: 0,
      schemaValidationMs: 0,
      serializationMs: 0,
      kafkaSendWaitMs: 0,
      batchesSent: 0,
      successfulSends: 0,
      failedSends: 0,
      averageBatchSendLatencyMs: 0,
      p95BatchSendLatencyMs: 0,
      inFlightPeak: 0,
      queueDepthPeak: 0,
      cpuUserMs: 0,
      cpuSystemMs: 0,
      rssMb: 0,
    },
  );
  console.log(
    JSON.stringify(
      {
        topic,
        groupId,
        producerWorkers,
        consumerWorkers,
        partitions,
        batchSize,
        batchConcurrency,
        maxInFlightRequests,
        targetEvents: events,
        actualEventsProduced: actualProduced,
        durationMs,
        aggregateEventsPerSecond: actualProduced / (durationMs / 1000),
        profiling: {
          ...profiling,
          averageBatchSendLatencyMs:
            profiling.successfulSends > 0
              ? profiling.kafkaSendWaitMs / profiling.successfulSends
              : 0,
          kafkaRequestRatePerSecond:
            profiling.successfulSends / Math.max(producerWallDurationMs / 1_000, 0.001),
        },
        producerCpuUtilization: {
          perWorkerAveragePercent: perWorkerCpu,
          aggregateAveragePercent:
            aggregateCpuSamples.length > 0
              ? aggregateCpuSamples.reduce((sum, value) => sum + value, 0) /
                aggregateCpuSamples.length
              : 0,
          aggregatePeakPercent: Math.max(...aggregateCpuSamples, 0),
        },
        partitionEventCounts: finalTopicOffsets.map((item) => Number(item.offset)),
        producerErrors: producerResults.filter((code) => code !== 0).length,
        validationFailures: summaries.reduce(
          (sum, item) => sum + (item.validationFailures ?? 0),
          0,
        ),
        consumerErrors: consumerMetrics.reduce(
          (sum, item) => sum + (item.kafkaConsumerErrors ?? 0) + (item.telemetryEventsFailed ?? 0),
          0,
        ),
        consumedProcessed: consumerMetrics.reduce(
          (sum, item) => sum + (item.telemetryEventsProcessed ?? 0),
          0,
        ),
        consumerThroughputPerSecond:
          consumerWorkers > 0 ? consumedProcessed / Math.max(durationMs / 1_000, 0.001) : 0,
        redisCommands,
        redisCommandsPerSecond:
          consumerWorkers > 0 ? redisCommands / Math.max(durationMs / 1_000, 0.001) : 0,
        consumerProfiling: {
          schemaValidationMs: consumerMetrics.reduce(
            (sum, item) => sum + (item.schemaValidationMs ?? 0),
            0,
          ),
          idempotencyLatencyMs: consumerMetrics.reduce(
            (sum, item) => sum + (item.idempotencyLatencyMs ?? 0),
            0,
          ),
          processingLatencyMs: consumerMetrics.reduce(
            (sum, item) => sum + (item.processingLatencyMs ?? 0),
            0,
          ),
          totalEventLatencyMs: consumerMetrics.reduce(
            (sum, item) => sum + (item.totalEventLatencyMs ?? 0),
            0,
          ),
          maxConcurrentHandlers: Math.max(
            ...consumerMetrics.map((item) => item.maxConcurrentHandlers ?? 0),
            0,
          ),
          cpuUtilizationPerWorker: Object.fromEntries(
            Object.entries(consumerCpuByWorker).map(([workerId, samples]) => [
              workerId,
              samples.reduce((sum, sample) => sum + sample.profile.cpuUtilizationPercent, 0) /
                samples.length,
            ]),
          ),
          aggregateAverageCpuPercent:
            aggregateConsumerCpuSamples.length > 0
              ? aggregateConsumerCpuSamples.reduce((sum, value) => sum + value, 0) /
                aggregateConsumerCpuSamples.length
              : 0,
          aggregatePeakCpuPercent: Math.max(...aggregateConsumerCpuSamples, 0),
        },
        lagSamples,
        consumerErrorLines: output.filter((line) =>
          /consumer (crashed|error)|CRASH|crash/i.test(line),
        ),
        consumerLag: lag,
        downstreamMode,
        downstreamMetrics,
        groupLags,
        groupMembers: group?.groups?.[0]?.members?.length ?? 0,
        allEventsConsumed:
          lag === 0 &&
          consumerMetrics.reduce((sum, item) => sum + (item.telemetryEventsProcessed ?? 0), 0) ===
            actualProduced,
        beforeResources,
        afterResources,
        resourceSamples,
      },
      null,
      2,
    ),
  );
} finally {
  if (resourceSampler) clearInterval(resourceSampler);
  await stopAll();
  await admin.deleteTopics({ topics: [topic] }).catch(() => undefined);
  await admin.disconnect();
}
