import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

function publicOrigin(req: NextRequest): string {
  const proto = (req.headers.get("x-forwarded-proto") || "https").split(",")[0].trim() || "https";
  const host =
    (req.headers.get("x-forwarded-host") || "").split(",")[0].trim() ||
    (req.headers.get("host") || "").split(",")[0].trim();
  if (host) return `${proto}://${host}`;
  return req.nextUrl.origin;
}

export async function POST(req: NextRequest) {
  const res = NextResponse.redirect(new URL("/login", publicOrigin(req)), { status: 303 });
  res.cookies.set({
    name: "scout_session",
    value: "",
    httpOnly: true,
    sameSite: "lax",
    secure: true,
    path: "/",
    maxAge: 0,
  });
  return res;
}

