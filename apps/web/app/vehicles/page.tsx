'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
const api = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:3000';
export default function Vehicles() {
  const [d, setD] = useState<any[]>([]);
  useEffect(() => {
    fetch(`${api}/api/v1/vehicles?limit=50`)
      .then((r) => r.json())
      .then((x) => setD(x.data || []));
  }, []);
  return (
    <>
      <h1>Vehicles</h1>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>VIN</th>
              <th>OEM</th>
              <th>Model</th>
              <th>Status</th>
              <th>Powertrain</th>
            </tr>
          </thead>
          <tbody>
            {d.map((v) => (
              <tr key={v.id}>
                <td>
                  <Link href={`/vehicles/${v.id}`}>{v.vin}</Link>
                </td>
                <td>{v.oem}</td>
                <td>{v.model}</td>
                <td>
                  <span className="tag">{v.status}</span>
                </td>
                <td>{v.powertrainType}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
