"use client";

import { Suspense, useEffect, useState, FormEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  getMe,
  applyUnlockCode,
  createCheckoutSession,
  createPortalSession,
  type UserInfo,
} from "../../lib/api";

const TIER_LABELS: Record<string, string> = {
  free: "Free",
  pro: "Pro",
  unlocked: "Unlocked",
};

const TIER_COLORS: Record<string, string> = {
  free: "#6b7280",
  pro: "#6366f1",
  unlocked: "#059669",
};

export default function SettingsPage() {
  return (
    <Suspense fallback={null}>
      <SettingsContent />
    </Suspense>
  );
}

function SettingsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [user, setUser] = useState<UserInfo | null>(null);
  const [code, setCode] = useState("");
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    getMe().then(setUser).catch(() => {});
  }, []);

  useEffect(() => {
    const checkout = searchParams.get("checkout");
    if (checkout === "success") {
      setSuccess("Subscription activated! You now have Pro access.");
      // Refresh user data to get updated tier.
      getMe().then(setUser).catch(() => {});
    }
  }, [searchParams]);

  async function handleUnlockCode(e: FormEvent) {
    e.preventDefault();
    setError("");
    setSuccess("");
    setLoading(true);
    try {
      const res = await applyUnlockCode(code);
      setSuccess(`Tier upgraded to ${TIER_LABELS[res.tier] || res.tier}!`);
      setCode("");
      getMe().then(setUser).catch(() => {});
    } catch (err: any) {
      const msg = err?.message || "";
      try {
        const parsed = JSON.parse(msg);
        setError(parsed.detail || "Invalid code");
      } catch {
        setError(msg.includes("invalid_code") ? "Invalid code" : msg || "Something went wrong");
      }
    } finally {
      setLoading(false);
    }
  }

  async function handleUpgrade() {
    setError("");
    setLoading(true);
    try {
      const res = await createCheckoutSession();
      if (res.checkout_url) {
        window.location.href = res.checkout_url;
      }
    } catch (err: any) {
      setError("Failed to start checkout. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  async function handleManageSubscription() {
    setError("");
    setLoading(true);
    try {
      const res = await createPortalSession();
      if (res.portal_url) {
        window.location.href = res.portal_url;
      }
    } catch (err: any) {
      setError("Failed to open subscription portal.");
    } finally {
      setLoading(false);
    }
  }

  async function handleLogout() {
    await fetch("/api/logout", { method: "POST" });
    router.push("/login");
  }

  const inputStyle = {
    border: "1px solid rgba(0,0,0,0.10)",
    borderRadius: 12,
    padding: "10px 12px",
    fontSize: 14,
    width: "100%",
    boxSizing: "border-box" as const,
  };

  const btnPrimary = {
    border: "none",
    borderRadius: 12,
    padding: "10px 16px",
    background: "#6366f1",
    color: "white",
    fontWeight: 900 as const,
    fontSize: 14,
    cursor: loading ? "wait" : ("pointer" as const),
    opacity: loading ? 0.7 : 1,
  };

  const btnSecondary = {
    ...btnPrimary,
    background: "white",
    color: "#374151",
    border: "1px solid rgba(0,0,0,0.10)",
    fontWeight: 600 as const,
  };

  const isPaid = user?.tier === "pro" || user?.tier === "unlocked";

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
          width: "min(480px, 95vw)",
          background: "white",
          border: "1px solid rgba(0,0,0,0.08)",
          borderRadius: 16,
          padding: 24,
          boxShadow: "0 4px 24px rgba(0,0,0,0.06)",
          display: "flex",
          flexDirection: "column",
          gap: 20,
        }}
      >
        {/* Header */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ fontWeight: 900, fontSize: 16, color: "#111827" }}>Account</div>
          <button
            onClick={() => router.push("/")}
            style={{
              border: "none",
              background: "none",
              color: "#6366f1",
              fontSize: 13,
              cursor: "pointer",
              fontWeight: 600,
            }}
          >
            Back to Scout
          </button>
        </div>

        {/* User info */}
        {user && (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <div style={{ fontSize: 13, color: "#6b7280" }}>Plan</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span
                style={{
                  display: "inline-block",
                  padding: "3px 10px",
                  borderRadius: 20,
                  fontSize: 12,
                  fontWeight: 700,
                  color: "white",
                  background: TIER_COLORS[user.tier] || "#6b7280",
                }}
              >
                {TIER_LABELS[user.tier] || user.tier}
              </span>
              {user.tier === "free" && (
                <span style={{ fontSize: 12, color: "#6b7280" }}>
                  3 active jobs &middot; scouting only
                </span>
              )}
              {isPaid && (
                <span style={{ fontSize: 12, color: "#6b7280" }}>
                  Unlimited jobs &middot; full access
                </span>
              )}
            </div>
          </div>
        )}

        {/* Upgrade to Pro */}
        {user?.tier === "free" && (
          <div
            style={{
              background: "#f5f3ff",
              borderRadius: 12,
              padding: 16,
              display: "flex",
              flexDirection: "column",
              gap: 10,
            }}
          >
            <div style={{ fontSize: 14, fontWeight: 700, color: "#4338ca" }}>
              Upgrade to Pro
            </div>
            <div style={{ fontSize: 13, color: "#6b7280" }}>
              Get unlimited jobs, phone enrichment, and full platform access.
            </div>
            <button onClick={handleUpgrade} disabled={loading} style={btnPrimary}>
              {loading ? "Loading..." : "Subscribe to Pro"}
            </button>
          </div>
        )}

        {/* Manage subscription (Pro users) */}
        {user?.tier === "pro" && user.stripe_subscription_status && (
          <button onClick={handleManageSubscription} disabled={loading} style={btnSecondary}>
            Manage subscription
          </button>
        )}

        {/* Unlock code */}
        {user?.tier === "free" && (
          <form
            onSubmit={handleUnlockCode}
            style={{ display: "flex", flexDirection: "column", gap: 10 }}
          >
            <div style={{ fontSize: 13, color: "#6b7280" }}>Have an unlock code?</div>
            <div style={{ display: "flex", gap: 8 }}>
              <input
                type="text"
                value={code}
                onChange={(e) => setCode(e.target.value)}
                placeholder="Enter code"
                style={{ ...inputStyle, flex: 1 }}
              />
              <button
                type="submit"
                disabled={loading || !code.trim()}
                style={{ ...btnSecondary, whiteSpace: "nowrap" as const }}
              >
                Apply
              </button>
            </div>
          </form>
        )}

        {/* Messages */}
        {error && <div style={{ color: "#b91c1c", fontSize: 13 }}>{error}</div>}
        {success && <div style={{ color: "#059669", fontSize: 13 }}>{success}</div>}

        {/* Logout */}
        <button onClick={handleLogout} style={{ ...btnSecondary, marginTop: 4 }}>
          Sign out
        </button>
      </div>
    </div>
  );
}
