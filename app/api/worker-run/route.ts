import { NextRequest, NextResponse } from "next/server";

const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET?.trim() ?? "";

function extractSharedSecret(request: NextRequest): string | undefined {
  const explicit = request.headers.get("x-worker-shared-secret")?.trim();
  if (explicit) return explicit;
  const auth = request.headers.get("authorization")?.trim();
  if (auth?.toLowerCase().startsWith("bearer ")) {
    return auth.slice(7).trim();
  }
  return undefined;
}

export async function POST(request: NextRequest) {
  if (WORKER_SHARED_SECRET) {
    const provided = extractSharedSecret(request);
    if (!provided || provided !== WORKER_SHARED_SECRET) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }
  }
  const runNowUrl = new URL("/api/run-now", request.url);
  const upstream = await fetch(runNowUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
    cache: "no-store",
  });
  const body = await upstream.text();
  const contentType = upstream.headers.get("content-type") ?? "application/json";
  return new NextResponse(body, {
    status: upstream.status,
    headers: {
      "content-type": contentType,
    },
  });
}

export async function GET() {
  return NextResponse.json(
    { message: "Use POST to trigger worker run" },
    { status: 405 }
  );
}
