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

  // Resolve a default user in the backend using a placeholder email.
  const defaultEmail = (process.env.DEFAULT_USER_EMAIL || "admin@scout.local").trim();
  const backendUrl = (
    process.env.BACKEND_URL ||
    process.env.NEXT_PUBLIC_BACKEND_URL ||
    "http://127.0.0.1:8000"
  ).replace(/\/+$/, "");
  const adminToken = (process.env.ADMIN_API_TOKEN || "").trim();

  let userId: string;
  try {
    const resolveRes = await fetch(`${backendUrl}/api/users/resolve`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(adminToken ? { Authorization: `Bearer ${adminToken}` } : {}),
      },
      body: JSON.stringify({ email: defaultEmail }),
    });
    if (!resolveRes.ok) {
      const data = await resolveRes.json().catch(() => ({}));
      console.error("User resolve failed:", resolveRes.status, data);
      return NextResponse.json(
        { error: data.detail || "Failed to resolve user" },
        { status: 500 }
      );
    }
    const userData = await resolveRes.json();
    userId = userData.id;
  } catch (err) {
    console.error("User resolve error:", err);
    return NextResponse.json(
      { error: "Failed to resolve user" },
      { status: 500 }
    );
  }

  // Create v2 session token
  const exp = Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60;
  const msg = `v2.${userId}.${exp}`;
  const token = `${msg}.${sign(secret, msg)}`;

  const res = NextResponse.json({ ok: true });
  res.cookies.set({
    name: "scout_session",
    value: token,
    httpOnly: true,
    sameSite: "lax",
    secure: true,
    path: "/",
    maxAge: 7 * 24 * 60 * 60,
  });
  return res;
}
