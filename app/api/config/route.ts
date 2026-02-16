import { NextResponse } from "next/server";
import { getConfig, setConfig, type CopyTraderConfig } from "@/lib/kv";

export async function GET() {
  try {
    const config = await getConfig();
    return NextResponse.json(config);
  } catch (e) {
    console.error("Config GET error:", e);
    return NextResponse.json(
      { error: "Failed to load config" },
      { status: 500 }
    );
  }
}

export async function PATCH(request: Request) {
  try {
    const body = (await request.json()) as Partial<CopyTraderConfig>;
    const config = await setConfig(body);
    return NextResponse.json(config);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Failed to save config";
    console.error("Config PATCH error:", e);
    return NextResponse.json(
      { error: msg },
      { status: 500 }
    );
  }
}
