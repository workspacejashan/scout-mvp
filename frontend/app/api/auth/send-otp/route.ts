import { NextRequest, NextResponse } from "next/server";
import { Resend } from "resend";
import { createOtp } from "../../../../lib/otp-store";

export const runtime = "nodejs";

// ---------------------------------------------------------------------------
// Work-email validation: block common free email providers
// ---------------------------------------------------------------------------

const FREE_EMAIL_DOMAINS = new Set([
  "gmail.com", "googlemail.com",
  "yahoo.com", "yahoo.co.uk", "yahoo.co.in",
  "outlook.com", "hotmail.com", "live.com", "msn.com",
  "aol.com",
  "icloud.com", "me.com", "mac.com",
  "protonmail.com", "proton.me", "pm.me",
  "tutanota.com", "tuta.io",
  "zoho.com", "zohomail.com",
  "yandex.com", "yandex.ru",
  "mail.com", "email.com",
  "gmx.com", "gmx.net",
  "fastmail.com",
  "hey.com",
  "mailinator.com",
  "guerrillamail.com",
  "tempmail.com",
]);

function isWorkEmail(email: string): boolean {
  const domain = email.split("@")[1]?.toLowerCase();
  if (!domain) return false;
  return !FREE_EMAIL_DOMAINS.has(domain);
}

export async function POST(req: NextRequest) {
  const apiKey = (process.env.RESEND_API_KEY || "").trim();
  if (!apiKey) {
    return NextResponse.json(
      { error: "Email service not configured" },
      { status: 500 }
    );
  }

  let body: { email?: string };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const email = (body.email || "").trim().toLowerCase();
  if (!email) {
    return NextResponse.json({ error: "Email is required" }, { status: 400 });
  }

  if (!isWorkEmail(email)) {
    return NextResponse.json(
      { error: "Please use your work email address" },
      { status: 400 }
    );
  }

  console.log(`[send-otp] email=${email}`);

  const result = await createOtp(email);
  if (!result.ok) {
    return NextResponse.json(
      { error: "Please wait before requesting another code", retryAfterMs: result.retryAfterMs },
      { status: 429 }
    );
  }

  try {
    const resend = new Resend(apiKey);
    const fromAddress =
      (process.env.FROM_EMAIL || "").trim() ||
      "Scout <onboarding@resend.dev>";

    const { data, error } = await resend.emails.send({
      from: fromAddress,
      to: email,
      subject: "Scout — Your sign-in code",
      text: `Your sign-in code is: ${result.code}\n\nThis code expires in 10 minutes. If you didn't request this, ignore this email.`,
      html: `<p>Your sign-in code is:</p><p style="font-size:32px;font-weight:bold;letter-spacing:6px;font-family:monospace">${result.code}</p><p>This code expires in 10 minutes. If you didn't request this, ignore this email.</p>`,
    });

    if (error) {
      console.error("Resend API error:", error);
      return NextResponse.json(
        { error: "Failed to send email" },
        { status: 500 }
      );
    }
  } catch (err) {
    console.error("Failed to send OTP email:", err);
    return NextResponse.json(
      { error: "Failed to send email" },
      { status: 500 }
    );
  }

  return NextResponse.json({ sent: true });
}
