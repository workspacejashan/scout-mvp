import crypto from "crypto";
import { NextRequest, NextResponse } from "next/server";
import { verifyOtp } from "../../../../lib/otp-store";

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
  const secret = (process.env.AUTH_SECRET || "").trim();
  if (!secret) {
    return NextResponse.json(
      { error: "Server misconfigured: missing AUTH_SECRET" },
      { status: 500 }
    );
  }

  let body: { email?: string; code?: string };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const email = (body.email || "").trim().toLowerCase();
  const code = (body.code || "").trim();

  if (!email || !code) {
    return NextResponse.json(
      { error: "Email and code are required" },
      { status: 400 }
    );
  }

  const result = await verifyOtp(email, code);
  if (!result.ok) {
    const messages: Record<string, string> = {
      invalid: "Invalid code",
      expired: "Code has expired. Please request a new one.",
      max_attempts: "Too many attempts. Please request a new code.",
    };
    return NextResponse.json(
      { error: messages[result.reason] },
      { status: 401 }
    );
  }

  // Resolve (or create) the user in the backend.
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
      body: JSON.stringify({ email }),
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

  // Create v2 session token with user identity: v2.<userId>.<exp>.<sig>
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
