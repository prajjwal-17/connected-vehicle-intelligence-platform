import './globals.css';
import Link from 'next/link';
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <nav>
          <strong>FleetPulse</strong>
          <span>
            <Link href="/">Dashboard</Link>
            <Link href="/vehicles">Vehicles</Link>
            <Link href="/alerts">Alerts</Link>
            <Link href="/analytics">Analytics</Link>
          </span>
        </nav>
        <main>{children}</main>
      </body>
    </html>
  );
}
