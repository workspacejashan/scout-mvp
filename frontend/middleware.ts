import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";

function base64UrlEncode(bytes: ArrayBuffer): string {
  const bin = String.fromCharCode(...new Uint8Array(bytes));
  const b64 = btoa(bin);
  return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function hmacSha256Base64Url(secret: string, message: string): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return base64UrlEncode(sig);
}

/**
 * Parse and validate the session cookie.
 * Supports two formats:
 *   v1.<exp>.<sig>              — legacy (no user identity)
 *   v2.<userId>.<exp>.<sig>     — current (includes user identity)
 *
 * Returns the userId if present, or null for legacy v1 sessions.
 * Returns false if the cookie is invalid/expired.
 */
async function parseSessionCookie(
  cookie: string,
  secret: string
): Promise<{ valid: true; userId: string | null } | { valid: false }> {
  const parts = String(cookie || "").split(".");

  if (parts[0] === "v2" && parts.length === 4) {
    // v2.<userId>.<exp>.<sig>
    const userId = parts[1];
    const exp = Number(parts[2]);
    if (!userId || !Number.isFinite(exp)) return { valid: false };
    const now = Math.floor(Date.now() / 1000);
    if (exp <= now) return { valid: false };

    const msg = `v2.${userId}.${parts[2]}`;
    const expected = await hmacSha256Base64Url(secret, msg);
    if (expected !== parts[3]) return { valid: false };
    return { valid: true, userId };
  }

  if (parts[0] === "v1" && parts.length === 3) {
    // Legacy v1.<exp>.<sig>
    const exp = Number(parts[1]);
    if (!Number.isFinite(exp)) return { valid: false };
    const now = Math.floor(Date.now() / 1000);
    if (exp <= now) return { valid: false };

    const msg = `v1.${parts[1]}`;
    const expected = await hmacSha256Base64Url(secret, msg);
    if (expected !== parts[2]) return { valid: false };
    return { valid: true, userId: null };
  }

  return { valid: false };
}

function isPublicPath(pathname: string): boolean {
  if (pathname === "/login") return true;
  if (pathname === "/api/login") return true;
  if (pathname === "/api/logout") return true;
  if (pathname === "/api/auth/send-otp") return true;
  if (pathname === "/api/auth/verify-otp") return true;
  if (pathname === "/api/auth/verify-access-code") return true;
  if (pathname.startsWith("/_next/")) return true;
  if (pathname === "/favicon.ico") return true;
  return false;
}

export async function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl;
  if (isPublicPath(pathname)) return NextResponse.next();

  const secret = (process.env.AUTH_SECRET || "").trim();
  // Fail closed if auth is configured but secret is missing.
  if (!secret) {
    return new NextResponse("server_misconfigured:missing_AUTH_SECRET", { status: 500 });
  }

  const cookie = req.cookies.get("scout_session")?.value || "";
  const session = await parseSessionCookie(cookie, secret);

  if (!session.valid) {
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.searchParams.set("next", pathname);
    return NextResponse.redirect(url);
  }

  // Forward user identity to downstream API routes via request headers.
  const requestHeaders = new Headers(req.headers);
  if (session.userId) {
    requestHeaders.set("x-user-id", session.userId);
  }

  return NextResponse.next({
    request: { headers: requestHeaders },
  });
}

export const config = {
  matcher: ["/((?!.*\\..*).*)"],
};
