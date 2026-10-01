'use client';
import { useEffect, useState } from 'react';
const api = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:3000';
export default function Alerts() {
  const [d, setD] = useState<any[]>([]);
  useEffect(() => {
    fetch(`${api}/api/v1/alerts?limit=50`)
      .then((r) => r.json())
      .then((x) => setD(x.data || []));
  }, []);
  return (
    <>
      <h1>Alerts</h1>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Type</th>
              <th>Severity</th>
              <th>Status</th>
              <th>Detected</th>
            </tr>
          </thead>
          <tbody>
            {d.map((a) => (
              <tr key={a.id}>
                <td>{a.alertType}</td>
                <td>{a.severity}</td>
                <td>{a.status}</td>
                <td>{new Date(a.detectedAt).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
