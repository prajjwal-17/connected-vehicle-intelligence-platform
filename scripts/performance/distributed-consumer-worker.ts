import pino from 'pino';
import { TelemetryIngestionConsumer, loadIngestionConfig } from '@fleetpulse/ingestion';

const config = loadIngestionConfig();
const logger = pino({
  level: config.logLevel,
  base: { service: 'distributed-benchmark-consumer' },
});
const consumer = new TelemetryIngestionConsumer(config, logger);
let stopped = false;
let previousCpu = process.cpuUsage();
let previousSampleAt = performance.now();
const metricsTimer = setInterval(() => {
  process.stdout.write(`${JSON.stringify({ metrics: consumer.metrics })}\n`);
  const now = performance.now();
  const cpu = process.cpuUsage(previousCpu);
  const elapsedMs = now - previousSampleAt;
  previousCpu = process.cpuUsage();
  previousSampleAt = now;
  process.stdout.write(
    `${JSON.stringify({
      profile: {
        workerId: process.env.BENCHMARK_WORKER_ID ?? process.pid,
        cpuUtilizationPercent: ((cpu.user + cpu.system) / 1_000 / elapsedMs) * 100,
        rssMb: process.memoryUsage().rss / (1024 * 1024),
      },
    })}\n`,
  );
}, 500);

const stop = async () => {
  if (stopped) return;
  stopped = true;
  clearInterval(metricsTimer);
  await consumer.stop();
  process.stdout.write(`${JSON.stringify({ ready: false, metrics: consumer.metrics })}\n`);
  process.exit(0);
};

process.once('SIGINT', () => void stop());
process.once('SIGTERM', () => void stop());
process.stdout.write(`${JSON.stringify({ ready: true, pid: process.pid })}\n`);
consumer.start().catch((error) => {
  process.stderr.write(`${JSON.stringify({ error: String(error) })}\n`);
  process.exitCode = 1;
});
