import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Bluey Dashboard",
  description: "Pattern analytics dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <nav>
          <span className="brand">Bluey</span>
          <a href="/patterns">Patterns</a>
          <a href={`/events/${todayStr()}`}>Events</a>
          <a href="/pipeline">Pipeline</a>
        </nav>
        <main>{children}</main>
      </body>
    </html>
  );
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}
