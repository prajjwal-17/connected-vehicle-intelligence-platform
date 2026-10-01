'use client';
import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
const api = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:3000';
export default function Vehicle() {
  const { id } = useParams<{ id: string }>();
  const [v, setV] = useState<any>();
  const [r, setR] = useState<any>();
  useEffect(() => {
    fetch(`${api}/api/v1/vehicles/${id}`)
      .then((x) => x.json())
      .then((x) => setV(x.data));
    fetch(`${api}/api/v1/vehicles/${id}/maintenance-risk`)
      .then((x) => (x.ok ? x.json() : null))
      .then((x) => setR(x?.data));
  }, [id]);
  return (
    <>
      <h1>Vehicle detail</h1>
      {v && (
        <>
          <p className="muted">
            {v.vin} · {v.oem} {v.model} · {v.status}
          </p>
          <div className="grid">
            <div className="card">
              <span className="muted">Telemetry events</span>
              <div className="metric">{v.telemetrySummary?.events ?? 0}</div>
            </div>
            <div className="card">
              <span className="muted">Maintenance risk</span>
              <div className="metric">
                {r ? `${Math.round(r.riskScore * 100)}%` : 'Unavailable'}
              </div>
              {r && <p>{r.prediction}</p>}
            </div>
          </div>
          <h2>Recent alerts</h2>
          {v.alerts?.map((a: any) => (
            <div className="card" key={a.id}>
              {a.alertType} · {a.severity} · {a.status}
            </div>
          ))}
        </>
      )}
    </>
  );
}
