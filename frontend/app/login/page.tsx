"use client";

import { useSearchParams, useRouter } from "next/navigation";
import { useState, FormEvent, Suspense } from "react";

function LoginForm() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const next = searchParams.get("next") || "/";

  const [accessCode, setAccessCode] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/auth/verify-access-code", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accessCode }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Invalid access code");
        return;
      }
      router.push(next);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setLoading(false);
    }
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
        <div style={{ fontSize: 13, color: "#6b7280" }}>
          Enter your access code to continue.
        </div>

        <form
          onSubmit={handleSubmit}
          style={{ display: "flex", flexDirection: "column", gap: 12 }}
        >
          <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <span style={{ fontSize: 12, fontWeight: 800, color: "#374151" }}>
              Access Code
            </span>
            <input
              type="password"
              autoFocus
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
