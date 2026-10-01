'use client';
import { useEffect, useState } from 'react';
const api = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:3000';
export default function Analytics() {
  const [d, setD] = useState<any>();
  useEffect(() => {
    fetch(`${api}/api/v1/analytics/summary`)
      .then((r) => r.json())
      .then((x) => setD(x.data));
  }, []);
  return (
    <>
      <h1>Fleet analytics</h1>
      <p className="muted">Last seven days of ClickHouse telemetry.</p>
      <div className="grid">
        <div className="card">
          <span className="muted">Events</span>
          <div className="metric">{d?.events ?? '—'}</div>
        </div>
        <div className="card">
          <span className="muted">Vehicles reporting</span>
          <div className="metric">{d?.vehicles ?? '—'}</div>
        </div>
        <div className="card">
          <span className="muted">Average speed</span>
          <div className="metric">
            {d?.averageSpeedKph ? `${Number(d.averageSpeedKph).toFixed(1)} km/h` : '—'}
          </div>
        </div>
      </div>
    </>
  );
}
