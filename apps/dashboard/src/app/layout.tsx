import type { Metadata } from "next";
import "./globals.css";
import Nav from "./Nav";
import Providers from "./providers";
import RouteFrame from "./RouteFrame";

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
        <Providers>
          <Nav />
          <RouteFrame>{children}</RouteFrame>
        </Providers>
      </body>
    </html>
  );
}
