"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export default function Nav() {
  const pathname = usePathname();
  const isActive = (path: string) =>
    pathname === path || (path !== "/" && pathname.startsWith(path + "/"));

  return (
    <nav>
      <Link href="/predictions" className={`brand ${isActive("/predictions") ? "active" : ""}`}>
        Bluey
      </Link>
      <Link href="/discovery-v2" className={`brand ${isActive("/discovery-v2") ? "active" : ""}`} style={{ marginLeft: "1rem" }}>
        Discovery v2
      </Link>
    </nav>
  );
}
