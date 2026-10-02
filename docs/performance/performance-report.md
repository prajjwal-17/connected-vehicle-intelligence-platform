# FleetPulse performance report

Run date: 2026-10-02. Measurements are local observations; no capacity claim is made.

## Producer profiling

Temporary producer-only profile for 10,000 events using 500-message batches:

| Stage                      |        Time |
| -------------------------- | ----------: |
| Event generation           |   105.98 ms |
| Explicit schema validation |    74.73 ms |
| JSON serialization         |    28.27 ms |
| Kafka send                 | 2,930.59 ms |
| Total measured stages      | 3,139.57 ms |

Kafka send is the dominant measured stage. The profile preserved `acks=-1`, retries, event IDs, schema headers, and `vehicleId` keys.

## Progressive producer-only results

Safe default: batch size 500, 1 MB byte cap, one batch in flight, compression enabled, producer `maxInFlightRequests=5`.

|   Target |                  Sample |       Actual | Schema failures | Status     |
| -------: | ----------------------: | -----------: | --------------: | ---------- |
|   1K/sec |  10,000 events / 10 sec | 6,000.78/sec |               0 | PASS       |
|   2K/sec |  20,000 events / 10 sec | 5,865.68/sec |               0 | PASS       |
|   5K/sec |  50,000 events / 10 sec | 6,363.69/sec |               0 | PASS       |
|  10K/sec | 100,000 events / 10 sec | 3,812.30/sec |               0 | PARTIAL    |
|  25K/sec |                 Not run |   UNVERIFIED |               — | UNVERIFIED |
|  50K/sec |                 Not run |   UNVERIFIED |               — | UNVERIFIED |
| 100K/sec |                 Not run |   UNVERIFIED |               — | UNVERIFIED |

Experimental five-batch concurrency reached 20,885.93/sec at a 5K target and 19,540.58/sec at a 10K target, but it is not the default because concurrent retries can weaken the existing cross-batch ordering guarantee.

## Full pipeline

A 1,000-event small run through Simulator → Kafka → ingestion → stream processor → ClickHouse completed at 307.98/sec with 0 schema failures. Ingestion, stream, and historical benchmark consumer groups all reached lag 0 across six partitions. ClickHouse contained 957,758 rows across 102 synthetic vehicles at verification time.

## Partition distribution

A 1,000-event sample using `vehicleId` as the Kafka key distributed as follows:

| Partition | Events |
| --------: | -----: |
|         0 |    200 |
|         1 |    180 |
|         2 |    150 |
|         3 |    160 |
|         4 |    160 |
|         5 |    150 |

## Hardware snapshot

The configured WSL limits were 3 processors, 4 GB memory, and 2 GB swap. A nearby Docker snapshot showed Kafka at 660 MiB/3.826 GiB and ClickHouse at 975 MiB/3.826 GiB; host CPU was approximately 8% and available host memory approximately 839 MB. These are snapshots, not a synchronized peak-utilization trace.

## API and ML reference measurements

The post-change API sample had 0 errors: overview p50/p95/p99 22.42/31.97/86.74 ms; vehicles 18.22/21.53/28.35 ms; vehicle detail 12.69/14.23/15.05 ms; alerts 18.67/21.04/21.23 ms; analytics 21.32/25.95/26.80 ms; maintenance risk 32.89/151.50/200.96 ms. ML remained unchanged at 7.82/8.51/44.64 ms p50/p95/p99 for 100 requests with 0 errors.

## Changes made

- Added bounded Kafka batching by message count and approximate payload bytes.
- Added bounded, configurable batch concurrency; the default remains one in-flight batch for ordering safety.
- Preserved durable Kafka acknowledgements, retries, schema validation, IDs, headers, keys, duplicate behavior, and backpressure.
- Prevented concurrent first-use producer calls from repeatedly initializing the Kafka topic.
- Kept historical ClickHouse batch inserts and API telemetry identifier/parallel-read fixes from the previous optimization pass.

## Remaining bottleneck

Kafka send time remains dominant. The safe ordering-preserving ceiling measured locally is approximately 6.36K events/sec; 10K was not stable. Higher targets and 3x burst were not attempted.
