import crypto from "crypto";
import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

function base64Url(buf: Buffer): string {
  return buf
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function sign(secret: string, message: string): string {
  const h = crypto.createHmac("sha256", secret).update(message).digest();
  return base64Url(h);
}

const DEFAULT_USER_ID = "access-code-user";

export async function POST(req: NextRequest) {
  const accessCode = (process.env.ACCESS_CODE || "").trim();
  if (!accessCode) {
    return NextResponse.json(
      { error: "Access code login is not enabled" },
      { status: 403 }
    );
  }

  const secret = (process.env.AUTH_SECRET || "").trim();
  if (!secret) {
    return NextResponse.json(
      { error: "Server misconfigured: missing AUTH_SECRET" },
      { status: 500 }
    );
  }

  let body: { accessCode?: string };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const code = (body.accessCode || "").trim();

  if (!code) {
    return NextResponse.json(
      { error: "Access code is required" },
      { status: 400 }
    );
  }

  if (code !== accessCode) {
    return NextResponse.json(
      { error: "Invalid access code" },
      { status: 401 }
    );
  }

  // Create v2 session token with a fixed user ID (no backend call needed)
  const exp = Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60;
  const msg = `v2.${DEFAULT_USER_ID}.${exp}`;
  const token = `${msg}.${sign(secret, msg)}`;

  const res = NextResponse.json({ ok: true });
  res.cookies.set({
    name: "scout_session",
    value: token,
    httpOnly: true,
    sameSite: "none",
    secure: true,
    path: "/",
    maxAge: 7 * 24 * 60 * 60,
  });
  return res;
}
