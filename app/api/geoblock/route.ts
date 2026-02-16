import { NextResponse } from "next/server";

/**
 * Proxies Polymarket's geoblock check so you can see what country/IP
 * Polymarket detects from THIS server's outbound requests.
 * Hit this from your Railway URL to verify the deployment region.
 */
export async function GET() {
  try {
    const res = await fetch("https://polymarket.com/api/geoblock");
    const data = await res.json();
    return NextResponse.json({
      ...data,
      _note: "This is what Polymarket sees from this server. If blocked=true or country=US, requests are coming from a restricted region.",
    });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}
