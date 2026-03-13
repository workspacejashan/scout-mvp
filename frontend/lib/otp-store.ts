import crypto from "crypto";
import { createClient, type RedisClientType } from "redis";

/**
 * Redis-backed OTP store with rate limiting, expiry, and attempt caps.
 *
 * Uses Redis for persistence (survives restarts) and multi-instance support.
 * Falls back to in-memory store if REDIS_URL is not configured.
 */

const OTP_TTL_SECONDS = 10 * 60; // 10 minutes
const OTP_LENGTH = 6;
const MAX_ATTEMPTS = 5;
const RATE_LIMIT_MS = 60 * 1000; // 1 OTP per email per 60 seconds

const KEY_PREFIX = "scout:otp:";

interface OtpEntry {
  code: string;
  email: string;
  attempts: number;
  createdAt: number;
}

// ---------------------------------------------------------------------------
// Redis client (lazy singleton)
// ---------------------------------------------------------------------------
let redisClient: RedisClientType | null = null;
let redisReady = false;
let redisInitAttempted = false;

async function getRedis(): Promise<RedisClientType | null> {
  const url = process.env.REDIS_URL;
  if (!url) return null;

  if (redisInitAttempted) return redisReady ? redisClient : null;
  redisInitAttempted = true;

  try {
    redisClient = createClient({ url }) as RedisClientType;
    redisClient.on("error", (err) => {
      console.error("[otp-store] Redis error:", err.message);
      redisReady = false;
    });
    redisClient.on("ready", () => {
      redisReady = true;
    });
    await redisClient.connect();
    redisReady = true;
    return redisClient;
  } catch (err) {
    console.error("[otp-store] Redis connection failed, using in-memory fallback:", err);
    redisReady = false;
    return null;
  }
}

// ---------------------------------------------------------------------------
// In-memory fallback (preserves original behavior when Redis is unavailable)
// ---------------------------------------------------------------------------
const memStore = new Map<string, OtpEntry & { expiresAt: number }>();

let cleanupScheduled = false;
function ensureCleanup() {
  if (cleanupScheduled) return;
  cleanupScheduled = true;
  setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of memStore) {
      if (entry.expiresAt <= now) memStore.delete(key);
    }
  }, 5 * 60 * 1000).unref();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function generateCode(): string {
  const buf = crypto.randomBytes(4);
  const num = buf.readUInt32BE(0) % 1_000_000;
  return String(num).padStart(OTP_LENGTH, "0");
}

function storeKey(email: string): string {
  return email.toLowerCase().trim();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
export type CreateResult =
  | { ok: true; code: string }
  | { ok: false; reason: "rate_limited"; retryAfterMs: number };

export async function createOtp(email: string): Promise<CreateResult> {
  const key = storeKey(email);
  const now = Date.now();
  const redis = await getRedis();

  if (redis) {
    // Redis path
    const existing = await redis.get(KEY_PREFIX + key);
    if (existing) {
      const entry: OtpEntry = JSON.parse(existing);
      const elapsed = now - entry.createdAt;
      if (elapsed < RATE_LIMIT_MS) {
        return { ok: false, reason: "rate_limited", retryAfterMs: RATE_LIMIT_MS - elapsed };
      }
    }

    const code = generateCode();
    const entry: OtpEntry = { code, email: key, attempts: 0, createdAt: now };
    await redis.setEx(KEY_PREFIX + key, OTP_TTL_SECONDS, JSON.stringify(entry));
    return { ok: true, code };
  }

  // In-memory fallback
  ensureCleanup();
  const existing = memStore.get(key);
  if (existing && existing.expiresAt > now) {
    const elapsed = now - existing.createdAt;
    if (elapsed < RATE_LIMIT_MS) {
      return { ok: false, reason: "rate_limited", retryAfterMs: RATE_LIMIT_MS - elapsed };
    }
  }

  const code = generateCode();
  memStore.set(key, {
    code,
    email: key,
    expiresAt: now + OTP_TTL_SECONDS * 1000,
    attempts: 0,
    createdAt: now,
  });
  return { ok: true, code };
}

export type VerifyResult =
  | { ok: true }
  | { ok: false; reason: "invalid" | "expired" | "max_attempts" };

export async function verifyOtp(email: string, code: string): Promise<VerifyResult> {
  const key = storeKey(email);
  const redis = await getRedis();

  if (redis) {
    const raw = await redis.get(KEY_PREFIX + key);
    if (!raw) return { ok: false, reason: "invalid" };

    const entry: OtpEntry = JSON.parse(raw);
    entry.attempts += 1;

    if (entry.attempts > MAX_ATTEMPTS) {
      await redis.del(KEY_PREFIX + key);
      return { ok: false, reason: "max_attempts" };
    }

    // Constant-time comparison
    const a = Buffer.from(entry.code);
    const b = Buffer.from(code);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      // Save incremented attempt count
      const ttl = await redis.ttl(KEY_PREFIX + key);
      if (ttl > 0) {
        await redis.setEx(KEY_PREFIX + key, ttl, JSON.stringify(entry));
      }
      return { ok: false, reason: "invalid" };
    }

    // Success — consume the OTP
    await redis.del(KEY_PREFIX + key);
    return { ok: true };
  }

  // In-memory fallback
  const now = Date.now();
  const entry = memStore.get(key);

  if (!entry) return { ok: false, reason: "invalid" };
  if (entry.expiresAt <= now) {
    memStore.delete(key);
    return { ok: false, reason: "expired" };
  }

  entry.attempts += 1;

  if (entry.attempts > MAX_ATTEMPTS) {
    memStore.delete(key);
    return { ok: false, reason: "max_attempts" };
  }

  const a = Buffer.from(entry.code);
  const b = Buffer.from(code);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return { ok: false, reason: "invalid" };
  }

  memStore.delete(key);
  return { ok: true };
}
