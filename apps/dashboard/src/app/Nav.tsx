"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export default function Nav() {
  const pathname = usePathname();
  const isActive = (path: string) =>
    pathname === path || (path !== "/" && pathname.startsWith(path + "/"));

  return (
    <nav>
      <Link href="/" className="brand">
        Bluey
      </Link>
      <Link href="/simulator" className={isActive("/simulator") ? "active" : ""}>
        Simulator
      </Link>
    </nav>
  );
}
