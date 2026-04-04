import { getServerSession } from "next-auth";
import { redirect } from "next/navigation";
import { getAuthOptions } from "@/lib/auth";
import SettingsForm from "./settings-form";

export default async function SettingsPage() {
  const session = await getServerSession(getAuthOptions());

  if (!session?.user?.id) {
    redirect("/");
  }

  return (
    <div className="card" style={{ maxWidth: 760 }}>
      <h1 style={{ marginTop: 0 }}>User Settings</h1>
      <p style={{ marginTop: 0 }}>
        Signed in as <strong>{session.user?.email ?? session.user?.name ?? "user"}</strong>.
        Save your context and preferences below.
      </p>
      <SettingsForm />
    </div>
  );
}
