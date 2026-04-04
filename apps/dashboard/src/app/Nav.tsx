"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { signIn, signOut, useSession } from "next-auth/react";

export default function Nav() {
  const pathname = usePathname();
  const { data: session } = useSession();
  const [hasLogo, setHasLogo] = useState(false);

  useEffect(() => {
    let mounted = true;
    const probe = new Image();
    probe.onload = () => {
      if (mounted) setHasLogo(true);
    };
    probe.onerror = () => {
      if (mounted) setHasLogo(false);
    };
    probe.src = "/logo.svg";

    return () => {
      mounted = false;
    };
  }, []);

  const isProd = process.env.NODE_ENV === "production";

  if (pathname === "/") {
    return null;
  }
  const isActive = (path: string) =>
    pathname === path || (path !== "/" && pathname.startsWith(path + "/"));
  const profileLabel = session?.user?.name?.trim() || session?.user?.email || "Profile";
  const onPredictions = isActive("/predictions");
  const brandContent = (
    <>
      {hasLogo ? (
        <img src="/logo.svg" alt="Bluey" className="brand-logo" />
      ) : (
        <span className="brand-fallback">Bluey</span>
      )}
    </>
  );

  return (
    <nav>
      <div className="nav-links">
        {onPredictions ? (
          <span className="brand">{brandContent}</span>
        ) : (
          <Link href="/predictions" className="brand">
            {brandContent}
          </Link>
        )}
        {session?.user ? (
          <>
            <Link href="/predictions" className={isActive("/predictions") ? "active" : ""}>
              Predictions
            </Link>
            {!isProd && (
              <>
                <Link href="/simulator" className={isActive("/simulator") ? "active" : ""}>
                  Simulator
                </Link>
                <Link href="/pattern-simulator" className={isActive("/pattern-simulator") ? "active" : ""}>
                  Pattern Sim
                </Link>
                <Link href="/predictions/governance" className={isActive("/predictions/governance") ? "active" : ""}>
                  Governance
                </Link>
              </>
            )}
          </>
        ) : null}
      </div>

      <div className="nav-profile">
        {session?.user ? (
          <>
            <Link href="/settings" className={isActive("/settings") ? "active profile-link" : "profile-link"}>
              {profileLabel}
            </Link>
            <button type="button" className="profile-action" onClick={() => signOut()}>
              Sign out
            </button>
          </>
        ) : (
          <>
            <button type="button" className="profile-action" onClick={() => signIn("google")}>
              Sign in with Google
            </button>
            <button type="button" className="profile-action" onClick={() => signIn("apple")}>
              Sign in with Apple
            </button>
          </>
        )}
      </div>
    </nav>
  );
}
