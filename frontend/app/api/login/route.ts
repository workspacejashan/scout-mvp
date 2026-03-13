import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

/**
 * Legacy /api/login endpoint.
 * Authentication now uses the email OTP flow (/api/auth/send-otp + /api/auth/verify-otp).
 * Any requests here redirect to the login page.
 */
export async function POST(req: NextRequest) {
  const proto = (req.headers.get("x-forwarded-proto") || "https").split(",")[0].trim() || "https";
  const host =
    (req.headers.get("x-forwarded-host") || "").split(",")[0].trim() ||
    (req.headers.get("host") || "").split(",")[0].trim();
  const origin = host ? `${proto}://${host}` : req.nextUrl.origin;

  return NextResponse.redirect(new URL("/login", origin), { status: 303 });
}

export async function GET(req: NextRequest) {
  return POST(req);
}
