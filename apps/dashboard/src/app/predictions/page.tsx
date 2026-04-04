import { redirect } from "next/navigation";
import { getServerSession } from "next-auth";
import { getAuthOptions } from "@/lib/auth";
import { PredictionsPage } from "../page";

export default async function PredictionsRoutePage() {
  const session = await getServerSession(getAuthOptions());
  if (!session?.user?.id) {
    redirect("/");
  }

  return <PredictionsPage />;
}
