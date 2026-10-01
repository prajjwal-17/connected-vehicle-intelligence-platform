'use client';
import { useEffect, useState } from 'react';
const api = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:3000';
function Assistant() {
  const [q, setQ] = useState('');
  const [a, setA] = useState('');
  return (
    <section className="card assistant">
      <strong>Fleet AI assistant</strong>
      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Ask about alerts or telemetry"
      />
      <button
        onClick={async () => {
          const r = await fetch(`${api}/api/v1/agent/query`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ question: q }),
          });
          setA((await r.json()).data?.answer || 'Unavailable');
        }}
      >
        Ask
      </button>
      {a && <p className="muted">{a}</p>}
    </section>
  );
}
export default function Home() {
  const [data, setData] = useState<any>();
  const [error, setError] = useState('');
  useEffect(() => {
    fetch(`${api}/api/v1/dashboard/overview`)
      .then((r) => r.json())
      .then((x) => setData(x.data))
      .catch(() => setError('API unavailable'));
  }, []);
  return (
    <>
      <h1>Fleet overview</h1>
      <p className="muted">Operational health across synthetic connected vehicles.</p>
      {error ? (
        <p className="error">{error}</p>
      ) : (
        <div className="grid">
          {[
            ['Vehicles', data?.totalVehicles],
            ['Active', data?.activeVehicles],
            ['Open alerts', data?.openAlerts],
            ['Critical alerts', data?.criticalOpenAlerts],
            ['Telemetry events', data?.telemetry?.events],
          ].map(([k, v]) => (
            <div className="card" key={String(k)}>
              <span className="muted">{k}</span>
              <div className="metric">{v ?? '—'}</div>
            </div>
          ))}
        </div>
      )}
      <Assistant />
    </>
  );
}
