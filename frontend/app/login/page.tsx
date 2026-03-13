"use client";

import { useSearchParams, useRouter } from "next/navigation";
import { useState, FormEvent, Suspense } from "react";

function LoginForm() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const next = searchParams.get("next") || "/";

  const [mode, setMode] = useState<"access-code" | "email">("access-code");
  const [step, setStep] = useState<"email" | "otp">("email");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [accessCode, setAccessCode] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleAccessCode(e: FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/auth/verify-access-code", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, accessCode }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Login failed");
        return;
      }
      router.push(next);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  async function handleSendOtp(e: FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/auth/send-otp", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Failed to send code");
        return;
      }
      setStep("otp");
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  async function handleVerifyOtp(e: FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/auth/verify-otp", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, code }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Verification failed");
        return;
      }
      router.push(next);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  function handleBack() {
    setStep("email");
    setCode("");
    setError("");
  }

  const inputStyle = {
    border: "1px solid rgba(0,0,0,0.10)",
    borderRadius: 12,
    padding: "10px 12px",
    fontSize: 14,
    width: "100%",
    boxSizing: "border-box" as const,
  };

  const buttonStyle = {
    border: "none",
    borderRadius: 12,
    padding: "10px 12px",
    background: "#6366f1",
    color: "white",
    fontWeight: 900,
    fontSize: 14,
    cursor: loading ? "wait" : "pointer",
    opacity: loading ? 0.7 : 1,
    width: "100%",
  };

  const tabStyle = (active: boolean) => ({
    border: "none",
    borderBottom: active ? "2px solid #6366f1" : "2px solid transparent",
    background: "none",
    padding: "8px 16px",
    fontSize: 13,
    fontWeight: active ? 700 : 400,
    color: active ? "#6366f1" : "#6b7280",
    cursor: "pointer",
  });

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        background: "#fafbfc",
        fontFamily:
          "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif",
      }}
    >
      <div
        style={{
          width: "min(420px, 95vw)",
          background: "white",
          border: "1px solid rgba(0,0,0,0.08)",
          borderRadius: 16,
          padding: 18,
          boxShadow: "0 4px 24px rgba(0,0,0,0.06)",
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        <div style={{ fontWeight: 900, fontSize: 16, color: "#111827" }}>
          Scout
        </div>

        <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #e5e7eb" }}>
          <button
            type="button"
            onClick={() => { setMode("access-code"); setError(""); setStep("email"); }}
            style={tabStyle(mode === "access-code")}
          >
            Access Code
          </button>
          <button
            type="button"
            onClick={() => { setMode("email"); setError(""); setStep("email"); }}
            style={tabStyle(mode === "email")}
          >
            Email OTP
          </button>
        </div>

        {mode === "access-code" ? (
          <>
            <div style={{ fontSize: 13, color: "#6b7280" }}>
              Enter your work email and access code to sign in.
            </div>
            <form
              onSubmit={handleAccessCode}
              style={{ display: "flex", flexDirection: "column", gap: 12 }}
            >
              <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <span style={{ fontSize: 12, fontWeight: 800, color: "#374151" }}>
                  Email
                </span>
                <input
                  type="email"
                  autoFocus
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@company.com"
                  style={inputStyle}
                />
              </label>
              <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <span style={{ fontSize: 12, fontWeight: 800, color: "#374151" }}>
                  Access Code
                </span>
                <input
                  type="password"
                  required
                  value={accessCode}
                  onChange={(e) => setAccessCode(e.target.value)}
                  placeholder="Enter access code"
                  style={inputStyle}
                />
              </label>
              {error && (
                <div style={{ color: "#b91c1c", fontSize: 12 }}>{error}</div>
              )}
              <button type="submit" disabled={loading} style={buttonStyle}>
                {loading ? "Signing in..." : "Sign in"}
              </button>
            </form>
          </>
        ) : (
          <>
            <div style={{ fontSize: 13, color: "#6b7280" }}>
              {step === "email"
                ? "Enter your work email to sign in or create an account."
                : `Enter the 6-digit code sent to ${email}`}
            </div>

            {step === "email" ? (
              <form
                onSubmit={handleSendOtp}
                style={{ display: "flex", flexDirection: "column", gap: 12 }}
              >
                <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  <span style={{ fontSize: 12, fontWeight: 800, color: "#374151" }}>
                    Email
                  </span>
                  <input
                    type="email"
                    autoFocus
                    required
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="you@company.com"
                    style={inputStyle}
                  />
                </label>
                {error && (
                  <div style={{ color: "#b91c1c", fontSize: 12 }}>{error}</div>
                )}
                <button type="submit" disabled={loading} style={buttonStyle}>
                  {loading ? "Sending..." : "Send code"}
                </button>
              </form>
            ) : (
              <form
                onSubmit={handleVerifyOtp}
                style={{ display: "flex", flexDirection: "column", gap: 12 }}
              >
                <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  <span style={{ fontSize: 12, fontWeight: 800, color: "#374151" }}>
                    Code
                  </span>
                  <input
                    type="text"
                    inputMode="numeric"
                    pattern="[0-9]{6}"
                    maxLength={6}
                    autoFocus
                    required
                    autoComplete="one-time-code"
                    value={code}
                    onChange={(e) => setCode(e.target.value.replace(/\D/g, ""))}
                    placeholder="000000"
                    style={{
                      ...inputStyle,
                      letterSpacing: 8,
                      textAlign: "center",
                      fontSize: 22,
                      fontFamily: "monospace",
                    }}
                  />
                </label>
                {error && (
                  <div style={{ color: "#b91c1c", fontSize: 12 }}>{error}</div>
                )}
                <button type="submit" disabled={loading} style={buttonStyle}>
                  {loading ? "Verifying..." : "Sign in"}
                </button>
                <button
                  type="button"
                  onClick={handleBack}
                  style={{
                    border: "none",
                    background: "none",
                    color: "#6366f1",
                    fontSize: 13,
                    cursor: "pointer",
                    padding: 4,
                  }}
                >
                  Use a different email
                </button>
              </form>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default function LoginPage() {
  return (
    <Suspense>
      <LoginForm />
    </Suspense>
  );
}
