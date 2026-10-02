const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3000';
const requests = Number(process.env.REQUESTS ?? 100);
const url = `${baseUrl}/api/v1/dashboard/overview`;
const samples = [];
let errors = 0;
for (let index = 0; index < requests; index += 1) {
  const start = performance.now();
  try {
    const response = await fetch(url);
    if (!response.ok) errors += 1;
    await response.arrayBuffer();
  } catch {
    errors += 1;
  }
  samples.push(performance.now() - start);
}
samples.sort((a, b) => a - b);
const percentile = (p) => samples[Math.min(samples.length - 1, Math.ceil(samples.length * p) - 1)];
console.log(
  JSON.stringify(
    {
      url,
      requests,
      errors,
      p50Ms: percentile(0.5),
      p95Ms: percentile(0.95),
      p99Ms: percentile(0.99),
      maxMs: samples.at(-1),
    },
    null,
    2,
  ),
);
/* global process, performance, fetch, console */
