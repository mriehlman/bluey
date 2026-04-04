"use client";

import { usePathname } from "next/navigation";

export default function RouteFrame({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  if (pathname === "/") {
    return <>{children}</>;
  }

  return <main>{children}</main>;
}
