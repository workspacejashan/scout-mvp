// IMPORTANT: Never call the backend directly from the browser in production.
// We proxy through Next.js so secrets (ADMIN_API_TOKEN) stay server-side.
const BACKEND = "/api/backend";

// ------------------------------------------------------------------------------
// Types
// ------------------------------------------------------------------------------

export interface Variant {
  id: string;
  entities: string[];
  boolean_text: string;
  selected: boolean;
}

export interface JobListItem {
  id: string;
  name: string;
  profile_count: number;
  phone_count: number;
  paused: boolean;
  created_at: string;
}

export interface Job {
  id: string;
  name: string;
  goal_text: string;
  status: "active" | "archived";
  profile_count: number;
  phone_count: number;
  paused: boolean;
  created_at: string;
  title_variants: Variant[];
  location_variants: Variant[];
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  created_at: string;
}

export interface SuggestionItem {
  entities: string[];
  boolean: string;
}

export interface CopilotResponse {
  job: {
    id: string;
    name: string;
    goal_text: string;
    status: "active" | "archived";
    created_at: string;
  };
  assistant_message: string;
  suggestions: {
    title_suggestions: SuggestionItem[];
    location_suggestions: SuggestionItem[];
    message: string;
  } | null;
  title_variants: Variant[];
  location_variants: Variant[];
}

export interface StrategyRun {
  id: string;
  job_id: string;
  title_variant_id: string;
  location_variant_id: string;
  boolean_text: string;
  status: "queued" | "running" | "completed" | "partial" | "failed";
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  pages_total: number;
  pages_completed: number;
  added_count: number;
  dropped_count: number;
  error_count: number;
  last_error: string | null;
}

export interface RunSelectedResponse {
  queued: StrategyRun[];
  skipped: number;
}

export interface EnrichJobResponse {
  source: EnrichSource;
  queued: number;
  skipped_in_flight: number;
  skipped_done: number;
}

export interface EnrichmentSummary {
  source: EnrichSource;
  total_profiles: number;
  total_records: number;
  queued: number;
  running: number;
  completed: number;
  failed: number;
  with_phone: number;
  last_error: string | null;
  updated_at: string;
}

export interface UploadProfilesResponse {
  job_id: string;
  apply_job_match: boolean;
  total_rows: number;
  created_profiles: number;
  existing_profiles: number;
  updated_existing: number;
  linked_to_job: number;
  skipped_duplicates: number;
  skipped_invalid: number;
  skipped_not_matching: number;
  errors: string[];
}

export interface UploadNewJobResponse {
  job_id: string;
  job_name: string;
  total_rows: number;
  created_profiles: number;
  existing_profiles: number;
  linked_to_job: number;
  skipped_duplicates: number;
  skipped_invalid: number;
  errors: string[];
}

export interface JobActivity {
  job_id: string;
  scouting_running: number;
  scouting_queued: number;
  enriching_running: number;
  enriching_queued: number;
}

export interface ActivityStatusResponse {
  jobs: Record<string, JobActivity>;
  total_active_jobs: number;
}

export interface PendingScoutingRun {
  id: string;
  title_variant_id: string;
  location_variant_id: string;
  created_at: string;
}

export interface PendingEnrichmentSourceCount {
  source: string;
  queued: number;
}

export interface PendingEnrichmentItem {
  enrichment_id: string;
  profile_id: string;
  name: string;
  city: string;
  state: string;
  created_at: string;
}

export interface JobPendingQueueResponse {
  job_id: string;
  paused: boolean;
  scouting_queued_count: number;
  scouting_queued: PendingScoutingRun[];
  enrichment_queued_count: number;
  enrichment_queued_by_source: PendingEnrichmentSourceCount[];
  enrichment_queued_sample: PendingEnrichmentItem[];
}

export interface JobProfileItem {
  id: string;
  name: string;
  linkedin_url: string;
  city: string;
  state: string;
  title: string;
  snippet: string;
  source: string;
}

export interface JobProfilesResponse {
  job_id: string;
  total: number;
  offset: number;
  limit: number;
  profiles: JobProfileItem[];
}

// Public UI-only identifiers (do not leak provider/vendor names to the browser).
export type EnrichSource = "source1" | "source2" | "source3" | "chain";

// SMS (Outreach + Inbox)
export interface OwnerSmsSettings {
  recruiter_company: string | null;
  twilio_from_number: string | null;
  sms_global_daily_limit: number;
  sms_business_start_hour: number;
  sms_business_end_hour: number;
}

export interface JobSmsSettings {
  job_id: string;
  job_location_label: string | null;
  sms_template_text: string | null;
  sms_daily_limit: number;
}

export interface SmsBatch {
  id: string;
  job_id: string;
  status: "queued" | "approved" | "completed" | "cancelled";
  requested_count: number;
  created_count: number;
  skipped_count: number;
  created_at: string;
  approved_at: string | null;
  completed_at: string | null;
}

export interface SmsCreateBatchResponse {
  batch_id: string;
  requested_count: number;
  created_count: number;
  skipped_count: number;
}

export interface SmsOutboundMessage {
  id: string;
  job_id: string;
  batch_id: string | null;
  profile_id: string | null;
  to_phone_e164: string;
  from_phone_e164: string;
  body: string;
  status: "queued" | "approved" | "sending" | "sent" | "failed";
  created_at: string;
  sent_at: string | null;
  error: string | null;
}

export interface SmsInboundMessage {
  id: string;
  job_id: string | null;
  from_phone_e164: string;
  to_phone_e164: string;
  body: string;
  tag: "Interested" | "Not Interested" | "Wrong Number" | "Ask Later" | "Unsubscribe" | "Unknown";
  received_at: string;
}

// ------------------------------------------------------------------------------
// API Calls
// ------------------------------------------------------------------------------

export class UpgradeRequiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UpgradeRequiredError";
  }
}

async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BACKEND}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  if (!res.ok) {
    const text = await res.text();
    if (res.status === 403) {
      let detail = text;
      try { detail = JSON.parse(text).detail || text; } catch {}
      throw new UpgradeRequiredError(detail);
    }
    throw new Error(text || res.statusText);
  }
  return res.json();
}

// Jobs
export async function listJobs(): Promise<JobListItem[]> {
  return api("/api/jobs");
}

export async function getJob(jobId: string): Promise<Job> {
  return api(`/api/jobs/${jobId}`);
}

export async function setJobPaused(jobId: string, paused: boolean): Promise<Job> {
  return api(`/api/jobs/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ paused }),
  });
}

export async function archiveJob(jobId: string): Promise<Job> {
  return api(`/api/jobs/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ status: "archived" }),
  });
}

export async function getJobChat(jobId: string): Promise<ChatMessage[]> {
  return api(`/api/jobs/${jobId}/chat`);
}

export async function listJobProfiles(
  jobId: string,
  offset: number = 0,
  limit: number = 100
): Promise<JobProfilesResponse> {
  const q = new URLSearchParams({ offset: String(offset), limit: String(limit) }).toString();
  return api(`/api/jobs/${jobId}/profiles?${q}`);
}

// Activity
export async function getActivityStatus(): Promise<ActivityStatusResponse> {
  return api("/api/activity/status");
}

export async function getJobPendingQueue(
  jobId: string,
  scoutLimit: number = 25,
  enrichLimit: number = 25
): Promise<JobPendingQueueResponse> {
  const q = new URLSearchParams({
    scout_limit: String(scoutLimit),
    enrich_limit: String(enrichLimit),
  }).toString();
  return api(`/api/jobs/${jobId}/pending-queue?${q}`);
}

// Copilot
export async function sendCopilotMessage(
  jobId: string | null,
  message: string
): Promise<CopilotResponse> {
  return api("/api/copilot/message", {
    method: "POST",
    body: JSON.stringify({ job_id: jobId, message }),
  });
}

export async function toggleTitleVariant(
  variantId: string,
  selected: boolean
): Promise<void> {
  await api("/api/copilot/toggle-title-variant", {
    method: "POST",
    body: JSON.stringify({ variant_id: variantId, selected }),
  });
}

export async function toggleLocationVariant(
  variantId: string,
  selected: boolean
): Promise<void> {
  await api("/api/copilot/toggle-location-variant", {
    method: "POST",
    body: JSON.stringify({ variant_id: variantId, selected }),
  });
}

// Strategy Runs
export async function runSelectedCombos(jobId: string): Promise<RunSelectedResponse> {
  return api("/api/strategy-runs/run-selected", {
    method: "POST",
    body: JSON.stringify({ job_id: jobId }),
  });
}

export async function getStrategyRun(runId: string): Promise<StrategyRun> {
  return api(`/api/strategy-runs/${runId}`);
}

export async function listStrategyRuns(jobId: string): Promise<StrategyRun[]> {
  return api(`/api/strategy-runs/job/${jobId}`);
}

export async function rerunStrategyRun(runId: string): Promise<StrategyRun> {
  return api(`/api/strategy-runs/rerun`, {
    method: "POST",
    body: JSON.stringify({ run_id: runId }),
  });
}

export async function resetStuckStrategyRuns(
  jobId: string,
  maxAgeMinutes: number = 30
): Promise<{ reset_count: number; reset_run_ids: string[]; enqueued_run_id: string | null }> {
  return api(`/api/strategy-runs/reset-stuck`, {
    method: "POST",
    body: JSON.stringify({ job_id: jobId, max_age_minutes: maxAgeMinutes }),
  });
}

export async function resumeQueueStrategyRuns(
  jobId: string
): Promise<{ enqueued_run_id: string | null }> {
  return api(`/api/strategy-runs/resume-queue`, {
    method: "POST",
    body: JSON.stringify({ job_id: jobId }),
  });
}

// Enrichment
export async function enrichJob(jobId: string, source: EnrichSource): Promise<EnrichJobResponse> {
  return api("/api/enrichment/enrich-job", {
    method: "POST",
    body: JSON.stringify({ job_id: jobId, source }),
  });
}

export async function getEnrichmentSummary(
  jobId: string,
  source: EnrichSource
): Promise<EnrichmentSummary> {
  const q = new URLSearchParams({ source }).toString();
  return api(`/api/enrichment/job/${jobId}/summary?${q}`);
}

export function getEnrichedDownloadUrl(jobId: string, source: EnrichSource): string {
  const q = new URLSearchParams({ source }).toString();
  return `${BACKEND}/api/enrichment/job/${jobId}/download?${q}`;
}

export function getBasicProfilesDownloadUrl(jobId: string, source: EnrichSource): string {
  const q = new URLSearchParams({ source }).toString();
  return `${BACKEND}/api/jobs/${jobId}/download-basic?${q}`;
}

// Upload
export async function uploadProfilesCsv(
  jobId: string,
  file: File,
  applyJobMatch: boolean
): Promise<UploadProfilesResponse> {
  const form = new FormData();
  form.append("file", file);
  form.append("apply_job_match", String(applyJobMatch));

  const res = await fetch(`${BACKEND}/api/jobs/${jobId}/upload-profiles`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export async function uploadNewJob(
  name: string,
  file: File
): Promise<UploadNewJobResponse> {
  const form = new FormData();
  form.append("name", name);
  form.append("file", file);

  const res = await fetch(`${BACKEND}/api/jobs/upload-new`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

// ------------------------------------------------------------------------------
// SMS (Outreach + Inbox)
// ------------------------------------------------------------------------------

export async function getOwnerSmsSettings(): Promise<OwnerSmsSettings> {
  return api("/api/sms/settings/owner");
}

export async function updateOwnerSmsSettings(
  payload: Partial<OwnerSmsSettings>
): Promise<OwnerSmsSettings> {
  return api("/api/sms/settings/owner", { method: "POST", body: JSON.stringify(payload) });
}

export async function getJobSmsSettings(jobId: string): Promise<JobSmsSettings> {
  return api(`/api/sms/settings/job/${jobId}`);
}

export async function updateJobSmsSettings(
  jobId: string,
  payload: Partial<JobSmsSettings>
): Promise<JobSmsSettings> {
  return api(`/api/sms/settings/job/${jobId}`, { method: "POST", body: JSON.stringify(payload) });
}

export async function createSmsBatch(
  jobId: string,
  requested_count: number
): Promise<SmsCreateBatchResponse> {
  return api("/api/sms/batches/create", {
    method: "POST",
    body: JSON.stringify({ job_id: jobId, requested_count }),
  });
}

export async function listSmsBatches(jobId: string): Promise<SmsBatch[]> {
  return api(`/api/sms/batches/job/${jobId}`);
}

export async function listSmsBatchMessages(batchId: string): Promise<SmsOutboundMessage[]> {
  return api(`/api/sms/batches/${batchId}/messages`);
}

export async function approveSmsBatch(batchId: string): Promise<{ batch_id: string; approved: boolean }> {
  return api(`/api/sms/batches/${batchId}/approve`, { method: "POST" });
}

export async function cancelSmsBatch(batchId: string): Promise<{ batch_id: string; approved: boolean }> {
  return api(`/api/sms/batches/${batchId}/cancel`, { method: "POST" });
}

export async function listSmsInbox(jobId?: string): Promise<SmsInboundMessage[]> {
  const q = jobId ? `?${new URLSearchParams({ job_id: jobId }).toString()}` : "";
  return api(`/api/sms/inbox${q}`);
}

// ---------------------------------------------------------------------------
// User / Account
// ---------------------------------------------------------------------------

export interface UserInfo {
  id: string;
  email: string;
  tier: "free" | "pro" | "unlocked";
  stripe_subscription_status: string | null;
}

export async function getMe(): Promise<UserInfo> {
  return api("/api/users/me");
}

export async function applyUnlockCode(code: string): Promise<{ tier: string }> {
  return api("/api/users/apply-unlock-code", {
    method: "POST",
    body: JSON.stringify({ code }),
  });
}

export async function createCheckoutSession(): Promise<{ checkout_url: string }> {
  return api("/api/billing/create-checkout-session", { method: "POST" });
}

export async function createPortalSession(): Promise<{ portal_url: string }> {
  return api("/api/billing/create-portal-session", { method: "POST" });
}