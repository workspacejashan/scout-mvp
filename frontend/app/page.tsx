"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChatMessage,
  EnrichSource,
  EnrichmentSummary,
  JobActivity,
  JobPendingQueueResponse,
  JobProfileItem,
  Job,
  JobListItem,
  StrategyRun,
  Variant,
  UpgradeRequiredError,
  UserInfo,
  archiveJob,
  enrichJob,
  getActivityStatus,
  getJobPendingQueue,
  getBasicProfilesDownloadUrl,
  getEnrichmentSummary,
  getJob,
  getJobChat,
  getStrategyRun,
  listStrategyRuns,
  listJobProfiles,
  listJobs,
  resetStuckStrategyRuns,
  resumeQueueStrategyRuns,
  rerunStrategyRun,
  runSelectedCombos,
  sendCopilotMessage,
  setJobPaused,
  toggleLocationVariant,
  toggleTitleVariant,
  uploadNewJob,
  uploadProfilesCsv,
  getMe,
} from "../lib/api";

export default function HomePage() {
  const [jobs, setJobs] = useState<JobListItem[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [chat, setChat] = useState<ChatMessage[]>([]);
  const [runsOpen, setRunsOpen] = useState(false);
  const [variantsExpanded, setVariantsExpanded] = useState(true);
  const [activeRunsExpanded, setActiveRunsExpanded] = useState(true);

  const [message, setMessage] = useState("");
  const [sending, setSending] = useState(false);

  const [titleVariants, setTitleVariants] = useState<Variant[]>([]);
  const [locationVariants, setLocationVariants] = useState<Variant[]>([]);

  const [toast, setToast] = useState<{ text: string; kind: "info" | "error" | "success" } | null>(null);
  const toastTimer = useRef<number | null>(null);
  const autoResumeQueueRef = useRef<Record<string, boolean>>({});

  const [activeRuns, setActiveRuns] = useState<StrategyRun[]>([]);
  const [jobRuns, setJobRuns] = useState<StrategyRun[]>([]);
  const [isRunning, setIsRunning] = useState(false);
  const [isEnriching, setIsEnriching] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [activeEnrichJobId, setActiveEnrichJobId] = useState<string | null>(null);
  const [enrichmentSummary, setEnrichmentSummary] = useState<EnrichmentSummary | null>(null);
  const chainSource: EnrichSource = "chain";
  const uploadInputRef = useRef<HTMLInputElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);

  const [sidebarOpen, setSidebarOpen] = useState(true);

  const [profilesOpen, setProfilesOpen] = useState(false);
  const [profilesLoading, setProfilesLoading] = useState(false);
  const [profiles, setProfiles] = useState<JobProfileItem[]>([]);
  const [profilesTotal, setProfilesTotal] = useState(0);
  const [profilesOffset, setProfilesOffset] = useState(0);
  const profilesLimit = 100;

  const [activity, setActivity] = useState<Record<string, JobActivity>>({});
  const [activeJobsCount, setActiveJobsCount] = useState(0);
  const [showActiveOnly, setShowActiveOnly] = useState(false);
  const [currentUser, setCurrentUser] = useState<UserInfo | null>(null);
  const [pauseBusyByJobId, setPauseBusyByJobId] = useState<Record<string, boolean>>({});
  const [selectedJobIds, setSelectedJobIds] = useState<Record<string, boolean>>({});
  const [bulkBusy, setBulkBusy] = useState(false);
  const [pendingQueue, setPendingQueue] = useState<JobPendingQueueResponse | null>(null);

  // Upload new job modal
  const [uploadModalOpen, setUploadModalOpen] = useState(false);
  const [uploadJobName, setUploadJobName] = useState("");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadBusy, setUploadBusy] = useState(false);
  const uploadNewInputRef = useRef<HTMLInputElement>(null);

  // (SMS Outreach/Inbox removed — scouting-only)

  const showToast = (text: string, kind: "info" | "error" | "success" = "info") => {
    setToast({ text, kind });
    if (toastTimer.current) window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 6000);
  };

  const handleApiError = (e: unknown) => {
    if (e instanceof UpgradeRequiredError) {
      showToast(e.message + " Go to Settings to upgrade.", "error");
      return;
    }
    showToast(String((e as any)?.message || e), "error");
  };

  const refreshJobs = async () => {
    const data = await listJobs();
    setJobs(data);
  };

  const selectedCount = Object.values(selectedJobIds).filter(Boolean).length;
  const clearSelection = () => setSelectedJobIds({});

  const bulkArchiveSelected = async () => {
    const ids = Object.entries(selectedJobIds)
      .filter(([, v]) => v)
      .map(([k]) => k);
    if (ids.length === 0 || bulkBusy) return;
    const ok = window.confirm(`Archive ${ids.length} job(s)? (Data stays in DB)`);
    if (!ok) return;
    setBulkBusy(true);
    try {
      for (const id of ids) {
        await archiveJob(id);
      }
      clearSelection();
      if (selectedJobId && ids.includes(selectedJobId)) {
        setSelectedJobId(null);
        setSelectedJob(null);
      }
      await refreshJobs();
      showToast("Archived", "success");
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    } finally {
      setBulkBusy(false);
    }
  };

  const bulkDeleteSelected = async () => {
    const ids = Object.entries(selectedJobIds)
      .filter(([, v]) => v)
      .map(([k]) => k);
    if (ids.length === 0 || bulkBusy) return;
    const ok = window.confirm(`Delete (hide) ${ids.length} job(s)? (Data stays in DB)`);
    if (!ok) return;
    // "Delete" here is a soft-hide = archive.
    await bulkArchiveSelected();
  };

  const refreshJobRuns = async (jobId: string) => {
    const runs = await listStrategyRuns(jobId);
    setJobRuns(runs);

    // Auto-resume: if some runs already completed/failed but the rest are stuck as queued,
    // kick the next queued run. No manual intervention required.
    try {
      const hasDone = runs.some((r) => r.status === "completed" || r.status === "partial" || r.status === "failed");
      const hasQueued = runs.some((r) => r.status === "queued");
      const hasRunning = runs.some((r) => r.status === "running");
      if (hasDone && hasQueued && !hasRunning && !autoResumeQueueRef.current[jobId]) {
        autoResumeQueueRef.current[jobId] = true;
        await resumeQueueStrategyRuns(jobId);
      }
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    refreshJobs().catch(() => {});
    getMe().then(setCurrentUser).catch(() => {});
    const id = window.setInterval(() => refreshJobs().catch(() => {}), 5000);
    return () => window.clearInterval(id);
  }, []);

  // Poll activity status
  useEffect(() => {
    const poll = async () => {
      try {
        const res = await getActivityStatus();
        setActivity(res.jobs);
        setActiveJobsCount(res.total_active_jobs);
      } catch {
        // silent fail
      }
    };
    poll();
    const id = window.setInterval(poll, 3000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    // Default behavior: ChatGPT-style desktop = sidebar open, mobile = closed.
    try {
      if (window.innerWidth < 900) setSidebarOpen(false);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    if (!selectedJobId) {
      setSelectedJob(null);
      setChat([]);
      setTitleVariants([]);
      setLocationVariants([]);
      setJobRuns([]);
      setPendingQueue(null);
      return;
    }
    (async () => {
      const j = await getJob(selectedJobId);
      const msgs = await getJobChat(selectedJobId);
      setSelectedJob(j);
      setChat(msgs);
      setTitleVariants(j.title_variants);
      setLocationVariants(j.location_variants);
      await refreshJobRuns(selectedJobId);
    })().catch((e) => {
      showToast(String(e?.message || e), "error");
    });
  }, [selectedJobId]);

  // Poll pending queue for selected job
  useEffect(() => {
    if (!selectedJobId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await getJobPendingQueue(selectedJobId, 20, 12);
        if (!cancelled) setPendingQueue(res);
      } catch {
        // silent fail
      }
    };
    poll();
    const id = window.setInterval(poll, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [selectedJobId]);

  // Keep the Runs modal fresh so "queued" doesn't look stuck when the worker is progressing.
  useEffect(() => {
    if (!selectedJobId) return;
    if (!runsOpen && !isRunning) return;
    const id = window.setInterval(() => refreshJobRuns(selectedJobId).catch(() => {}), 5000);
    return () => window.clearInterval(id);
  }, [selectedJobId, runsOpen, isRunning]);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chat]);

  const openProfiles = async () => {
    if (!selectedJobId) return;
    setProfilesOpen(true);
    setProfilesLoading(true);
    try {
      const res = await listJobProfiles(selectedJobId, 0, profilesLimit);
      setProfiles(res.profiles);
      setProfilesTotal(res.total);
      setProfilesOffset(res.profiles.length);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    } finally {
      setProfilesLoading(false);
    }
  };

  const loadMoreProfiles = async () => {
    if (!selectedJobId || profilesLoading) return;
    if (profiles.length >= profilesTotal) return;
    setProfilesLoading(true);
    try {
      const res = await listJobProfiles(selectedJobId, profilesOffset, profilesLimit);
      setProfiles((prev) => [...prev, ...res.profiles]);
      setProfilesTotal(res.total);
      setProfilesOffset((prev) => prev + res.profiles.length);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    } finally {
      setProfilesLoading(false);
    }
  };


  const send = async () => {
    const msg = message.trim();
    if (!msg || sending) return;
    setSending(true);
    setMessage("");
    try {
      const res = await sendCopilotMessage(selectedJobId, msg);
      setSelectedJobId(res.job.id);
      setTitleVariants(res.title_variants);
      setLocationVariants(res.location_variants);
      await refreshJobs();
      const msgs = await getJobChat(res.job.id);
      setChat(msgs);
    } catch (e: any) {
      handleApiError(e);
    } finally {
      setSending(false);
    }
  };

  const handleToggleTitle = async (v: Variant) => {
    const newSelected = !v.selected;
    setTitleVariants((prev) =>
      prev.map((t) => (t.id === v.id ? { ...t, selected: newSelected } : t))
    );
    try {
      await toggleTitleVariant(v.id, newSelected);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    }
  };

  const handleToggleLocation = async (v: Variant) => {
    const newSelected = !v.selected;
    setLocationVariants((prev) =>
      prev.map((l) => (l.id === v.id ? { ...l, selected: newSelected } : l))
    );
    try {
      await toggleLocationVariant(v.id, newSelected);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    }
  };

  const runSelected = async () => {
    if (!selectedJobId || isRunning) return;
    const selectedTitles = titleVariants.filter((t) => t.selected);
    const selectedLocations = locationVariants.filter((l) => l.selected);
    if (!selectedTitles.length || !selectedLocations.length) {
      showToast("Select at least one title and one location", "error");
      return;
    }

    setIsRunning(true);
    showToast(`Running ${selectedTitles.length} × ${selectedLocations.length} combos...`, "info");

    try {
      const res = await runSelectedCombos(selectedJobId);
      if (res.queued.length === 0 && res.skipped > 0) {
        showToast(`All ${res.skipped} combos already ran`, "info");
        setIsRunning(false);
        return;
      }
      setActiveRuns(res.queued);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
      setIsRunning(false);
    }
  };

  const startEnrich = async () => {
    if (!selectedJobId || isEnriching) return;
    setIsEnriching(true);
    setActiveEnrichJobId(selectedJobId);
    setEnrichmentSummary(null);
    showToast("Enriching profiles...", "info");
    try {
      const res = await enrichJob(selectedJobId, chainSource);
      if (res.queued === 0) {
        if (res.skipped_done > 0) {
          showToast("Already enriched", "info");
        } else {
          showToast("Nothing to enrich yet", "info");
        }
        setIsEnriching(false);
        setActiveEnrichJobId(null);
        return;
      }
      showToast(`Queued ${res.queued} profiles for phone lookup`, "info");
    } catch (e: any) {
      handleApiError(e);
      setIsEnriching(false);
      setActiveEnrichJobId(null);
    }
  };

  const startUpload = () => {
    if (!selectedJobId || isUploading) return;
    uploadInputRef.current?.click();
  };

  const handleUploadFile = async (file: File) => {
    if (!selectedJobId || isUploading) return;

    const applyFilters = window.confirm(
      "Apply job title filter (selected title variants) to uploaded profiles?\n\nOK = Yes (recommended)\nCancel = No (import all)"
    );

    setIsUploading(true);
    showToast("Uploading profiles...", "info");
    try {
      const res = await uploadProfilesCsv(selectedJobId, file, applyFilters);
      await refreshJobs();
      if (selectedJobId) {
        const j = await getJob(selectedJobId);
        setSelectedJob(j);
      }
      showToast(
        `Uploaded. Linked ${res.linked_to_job}. Skipped invalid ${res.skipped_invalid}. Skipped not-matching ${res.skipped_not_matching}.`,
        "success"
      );
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    } finally {
      setIsUploading(false);
    }
  };

  const handleUploadNewJob = async () => {
    if (!uploadJobName.trim() || !uploadFile || uploadBusy) return;

    setUploadBusy(true);
    showToast("Creating job and uploading profiles...", "info");
    try {
      const res = await uploadNewJob(uploadJobName.trim(), uploadFile);
      await refreshJobs();
      setSelectedJobId(res.job_id);
      setUploadModalOpen(false);
      setUploadJobName("");
      setUploadFile(null);
      showToast(
        `Created "${res.job_name}" with ${res.linked_to_job} profiles. Skipped invalid: ${res.skipped_invalid}.`,
        "success"
      );
    } catch (e: any) {
      handleApiError(e);
    } finally {
      setUploadBusy(false);
    }
  };

  // Poll active runs
  useEffect(() => {
    if (!activeRuns.length) return;

    let cancelled = false;
    const poll = async () => {
      const updated: StrategyRun[] = [];
      let allDone = true;
      let totalAdded = 0;

      for (const run of activeRuns) {
        const r = await getStrategyRun(run.id);
        updated.push(r);
        if (r.status === "queued" || r.status === "running") {
          allDone = false;
        }
        totalAdded += r.added_count;
      }

      if (cancelled) return;
      setActiveRuns(updated);

      if (allDone) {
        await refreshJobs();
        if (selectedJobId) {
          const j = await getJob(selectedJobId);
          setSelectedJob(j);
          await refreshJobRuns(selectedJobId);
        }
        showToast(`Done! Added ${totalAdded} profiles`, "success");
        setIsRunning(false);
        setActiveRuns([]);
      }
    };

    const id = window.setInterval(() => poll().catch(() => {}), 2000);
    poll().catch(() => {});

    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [activeRuns.length, selectedJobId]);

  // Poll enrichment summary
  useEffect(() => {
    if (!isEnriching || !activeEnrichJobId) return;

    let cancelled = false;
    const poll = async () => {
      const s = await getEnrichmentSummary(activeEnrichJobId, chainSource);
      if (cancelled) return;
      setEnrichmentSummary(s);

      const done = s.queued === 0 && s.running === 0 && s.total_records > 0;
      if (done) {
        await refreshJobs();
        if (selectedJobId) {
          const j = await getJob(selectedJobId);
          setSelectedJob(j);
        }
        if (s.with_phone > 0) {
          showToast(`Done! Found ${s.with_phone} profiles with phones`, "success");
        } else if (s.failed > 0) {
          showToast(s.last_error || "Enrichment failed", "error");
        } else {
          showToast("Enrichment done", "success");
        }
        setIsEnriching(false);
        setActiveEnrichJobId(null);
      }
    };

    const id = window.setInterval(() => poll().catch(() => {}), 2000);
    poll().catch(() => {});

    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [isEnriching, activeEnrichJobId, selectedJobId, chainSource]);

  const selectedTitleCount = titleVariants.filter((t) => t.selected).length;
  const selectedLocationCount = locationVariants.filter((l) => l.selected).length;
  const comboCount = selectedTitleCount * selectedLocationCount;
  const hasVariants = titleVariants.length > 0 || locationVariants.length > 0;

  const titleLabelById = useMemo(() => {
    const m = new Map<string, string>();
    for (const v of titleVariants) {
      m.set(v.id, (v.boolean_text || "").trim() || v.entities.join(" + "));
    }
    return m;
  }, [titleVariants]);

  const locationLabelById = useMemo(() => {
    const m = new Map<string, string>();
    for (const v of locationVariants) {
      m.set(v.id, (v.boolean_text || "").trim() || v.entities.join(", "));
    }
    return m;
  }, [locationVariants]);

  const doRerun = async (runId: string) => {
    if (!selectedJobId) return;
    try {
      const r = await rerunStrategyRun(runId);
      showToast("Rerun queued", "success");
      // Track it in the live progress list as well
      setActiveRuns((prev) => [r, ...prev]);
      await refreshJobRuns(selectedJobId);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    }
  };

  const doResetStuck = async () => {
    if (!selectedJobId) return;
    try {
      const res = await resetStuckStrategyRuns(selectedJobId, 30);
      if (res.reset_count > 0) {
        showToast(`Reset ${res.reset_count} stuck run(s)`, "success");
      } else {
        showToast("No stuck runs found", "info");
      }
      await refreshJobRuns(selectedJobId);
    } catch (e: any) {
      showToast(String(e?.message || e), "error");
    }
  };

  const parseDbTimestampUtc = (s: string | null | undefined): number | null => {
    if (!s) return null;
    // Backend returns timestamps like "2026-01-12T04:52:47.830865" (microseconds, no timezone).
    // JS Date can't parse microseconds reliably; normalize to milliseconds + force UTC.
    const trimmed = String(s)
      .trim()
      .replace(/(\.\d{3})\d+$/, "$1"); // .830865 -> .830
    const iso = trimmed.endsWith("Z") ? trimmed : `${trimmed}Z`;
    const t = new Date(iso).getTime();
    return Number.isFinite(t) ? t : null;
  };

  return (
    <div style={{ display: "flex", height: "100vh", width: "100vw", overflow: "hidden" }}>
      {toast && (
        <div
          style={{
            position: "fixed",
            right: 24,
            top: 24,
            background:
              toast.kind === "error"
                ? "var(--error)"
                : toast.kind === "success"
                ? "var(--success)"
                : "var(--accent)",
            color: "white",
            padding: "12px 18px",
            borderRadius: "var(--radius-sm)",
            boxShadow: "var(--shadow-lg)",
            fontSize: 14,
            animation: "slideDown 0.3s ease-out",
            zIndex: 1000,
          }}
        >
          {toast.text}
        </div>
      )}

      {profilesOpen && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            zIndex: 1100,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 24,
          }}
          onClick={() => setProfilesOpen(false)}
        >
          <div
            style={{
              width: "min(980px, 95vw)",
              height: "min(680px, 90vh)",
              background: "var(--panel)",
              border: "1px solid var(--border)",
              borderRadius: 16,
              boxShadow: "var(--shadow-lg)",
              overflow: "hidden",
              display: "flex",
              flexDirection: "column",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div
              style={{
                padding: 14,
                borderBottom: "1px solid var(--border)",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 12,
              }}
            >
              <div style={{ minWidth: 0 }}>
                <div style={{ fontWeight: 800, color: "var(--text)" }}>Profiles</div>
                <div style={{ fontSize: 12, color: "var(--muted)" }}>
                  {profilesTotal > 0 ? `${Math.min(profiles.length, profilesTotal)} / ${profilesTotal}` : "0"}
                </div>
              </div>
              <button
                onClick={() => setProfilesOpen(false)}
                style={{
                  background: "transparent",
                  border: "1px solid var(--border)",
                  color: "var(--muted)",
                  borderRadius: 10,
                  padding: "6px 10px",
                  fontSize: 13,
                }}
              >
                Close
              </button>
            </div>

            <div style={{ flex: 1, overflow: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr style={{ background: "var(--bg)" }}>
                    {["Name", "Location", "Title", "LinkedIn"].map((h) => (
                      <th
                        key={h}
                        style={{
                          textAlign: "left",
                          padding: "10px 12px",
                          borderBottom: "1px solid var(--border)",
                          color: "var(--muted)",
                          fontWeight: 700,
                        }}
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {profiles.map((p) => (
                    <tr key={p.id}>
                      <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>{p.name}</td>
                      <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                        {[p.city, p.state].filter(Boolean).join(", ")}
                      </td>
                      <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                        {(p.title || p.snippet || "").slice(0, 120)}
                      </td>
                      <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                        {p.linkedin_url ? (
                          <a href={p.linkedin_url} target="_blank" rel="noreferrer">
                            Open
                          </a>
                        ) : (
                          ""
                        )}
                      </td>
                    </tr>
                  ))}
                  {profiles.length === 0 && !profilesLoading && (
                    <tr>
                      <td colSpan={4} style={{ padding: 16, color: "var(--muted)" }}>
                        No profiles yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div
              style={{
                padding: 12,
                borderTop: "1px solid var(--border)",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              <div style={{ fontSize: 12, color: "var(--muted)" }}>{profilesLoading ? "Loading..." : ""}</div>
              <button
                onClick={loadMoreProfiles}
                disabled={profilesLoading || profiles.length >= profilesTotal}
                style={{
                  background: profiles.length < profilesTotal ? "white" : "var(--bg)",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  fontWeight: 700,
                  opacity: profilesLoading || profiles.length >= profilesTotal ? 0.6 : 1,
                  cursor: profilesLoading || profiles.length >= profilesTotal ? "not-allowed" : "pointer",
                }}
              >
                Load more
              </button>
            </div>
          </div>
        </div>
      )}

      {/* (Outreach removed — scouting-only) */}

      {/* (Inbox removed — scouting-only) */}

      {/* Upload New Job Modal */}
      {uploadModalOpen && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            zIndex: 1100,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 24,
          }}
          onClick={() => {
            if (!uploadBusy) {
              setUploadModalOpen(false);
              setUploadJobName("");
              setUploadFile(null);
            }
          }}
        >
          <div
            style={{
              width: "min(480px, 95vw)",
              background: "var(--panel)",
              border: "1px solid var(--border)",
              borderRadius: 16,
              boxShadow: "var(--shadow-lg)",
              overflow: "hidden",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div
              style={{
                padding: 20,
                borderBottom: "1px solid var(--border)",
              }}
            >
              <div style={{ fontWeight: 800, fontSize: 18, color: "var(--text)" }}>
                Upload Profiles
              </div>
              <div style={{ fontSize: 13, color: "var(--muted)", marginTop: 4 }}>
                Create a new list from a CSV file
              </div>
            </div>

            <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 16 }}>
              <div>
                <label
                  style={{
                    display: "block",
                    fontSize: 13,
                    fontWeight: 700,
                    color: "var(--text)",
                    marginBottom: 6,
                  }}
                >
                  Name this list
                </label>
                <input
                  type="text"
                  value={uploadJobName}
                  onChange={(e) => setUploadJobName(e.target.value)}
                  placeholder="e.g., Miami Real Estate Leads"
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: 10,
                    border: "1px solid var(--border)",
                    background: "var(--bg)",
                    fontSize: 14,
                    color: "var(--text)",
                  }}
                  disabled={uploadBusy}
                  autoFocus
                />
              </div>

              <div>
                <label
                  style={{
                    display: "block",
                    fontSize: 13,
                    fontWeight: 700,
                    color: "var(--text)",
                    marginBottom: 6,
                  }}
                >
                  CSV file
                </label>
                <input
                  ref={uploadNewInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  style={{ display: "none" }}
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    e.target.value = "";
                    if (f) setUploadFile(f);
                  }}
                  disabled={uploadBusy}
                />
                <button
                  onClick={() => uploadNewInputRef.current?.click()}
                  disabled={uploadBusy}
                  style={{
                    width: "100%",
                    padding: "24px 16px",
                    borderRadius: 10,
                    border: "2px dashed var(--border)",
                    background: uploadFile ? "var(--accent-soft)" : "var(--bg)",
                    color: uploadFile ? "var(--accent)" : "var(--muted)",
                    fontSize: 14,
                    fontWeight: 600,
                    cursor: uploadBusy ? "not-allowed" : "pointer",
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  {uploadFile ? (
                    <>
                      <span style={{ fontSize: 24 }}>✓</span>
                      <span>{uploadFile.name}</span>
                    </>
                  ) : (
                    <>
                      <span style={{ fontSize: 24 }}>📄</span>
                      <span>Drop CSV here or click to browse</span>
                    </>
                  )}
                </button>
                <div style={{ fontSize: 12, color: "var(--muted)", marginTop: 8 }}>
                  Required columns: name (or full_name), city, state
                </div>
              </div>
            </div>

            <div
              style={{
                padding: 16,
                borderTop: "1px solid var(--border)",
                display: "flex",
                justifyContent: "flex-end",
                gap: 10,
              }}
            >
              <button
                onClick={() => {
                  setUploadModalOpen(false);
                  setUploadJobName("");
                  setUploadFile(null);
                }}
                disabled={uploadBusy}
                style={{
                  padding: "10px 16px",
                  borderRadius: 10,
                  border: "1px solid var(--border)",
                  background: "white",
                  color: "var(--text)",
                  fontSize: 14,
                  fontWeight: 700,
                  cursor: uploadBusy ? "not-allowed" : "pointer",
                  opacity: uploadBusy ? 0.6 : 1,
                }}
              >
                Cancel
              </button>
              <button
                onClick={handleUploadNewJob}
                disabled={!uploadJobName.trim() || !uploadFile || uploadBusy}
                style={{
                  padding: "10px 20px",
                  borderRadius: 10,
                  border: "none",
                  background:
                    uploadJobName.trim() && uploadFile && !uploadBusy
                      ? "var(--accent)"
                      : "var(--border)",
                  color:
                    uploadJobName.trim() && uploadFile && !uploadBusy
                      ? "white"
                      : "var(--muted)",
                  fontSize: 14,
                  fontWeight: 700,
                  cursor:
                    uploadJobName.trim() && uploadFile && !uploadBusy
                      ? "pointer"
                      : "not-allowed",
                }}
              >
                {uploadBusy ? "Uploading..." : "Upload"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Sidebar */}
      {sidebarOpen && (
        <aside
          style={{
            width: 320,
            minWidth: 260,
            maxWidth: 360,
            height: "100vh",
            background: "rgba(0,0,0,0.02)",
            borderRight: "1px solid var(--border)",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              padding: 16,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 12,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 12, minWidth: 0 }}>
              <div
                style={{
                  width: 32,
                  height: 32,
                  borderRadius: 10,
                  background: "linear-gradient(135deg, #6366f1, #8b5cf6)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "white",
                  fontWeight: 800,
                  fontSize: 15,
                  flex: "0 0 auto",
                }}
              >
                S
              </div>
              <div style={{ minWidth: 0 }}>
                <div style={{ fontWeight: 700, color: "var(--text)", lineHeight: 1.1 }}>Scout</div>
                <div style={{ fontSize: 12, color: "var(--muted)" }}>Search history</div>
              </div>
            </div>

            <button
              onClick={() => setSidebarOpen(false)}
              style={{
                background: "transparent",
                border: "1px solid var(--border)",
                color: "var(--muted)",
                borderRadius: 10,
                padding: "6px 10px",
                fontSize: 13,
              }}
              aria-label="Close sidebar"
              title="Close sidebar"
            >
              ◀
            </button>
          </div>

          {/* User tier + settings link */}
          {currentUser && (
            <div
              style={{
                padding: "0 16px 12px",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                fontSize: 12,
              }}
            >
              <span
                style={{
                  padding: "2px 8px",
                  borderRadius: 20,
                  fontWeight: 700,
                  color: "white",
                  background:
                    currentUser.tier === "pro"
                      ? "#6366f1"
                      : currentUser.tier === "unlocked"
                      ? "#059669"
                      : "#6b7280",
                }}
              >
                {currentUser.tier === "free"
                  ? "Free"
                  : currentUser.tier === "pro"
                  ? "Pro"
                  : "Unlocked"}
              </span>
              <a
                href="/settings"
                style={{ color: "var(--muted)", textDecoration: "none", fontWeight: 600 }}
              >
                Settings
              </a>
            </div>
          )}

          <div style={{ padding: "0 16px 12px" }}>
            {selectedCount > 0 ? (
              <div
                style={{
                  display: "flex",
                  gap: 8,
                  padding: "4px",
                  background: "var(--panel)",
                  borderRadius: 12,
                  animation: "slideDown 0.2s ease-out",
                }}
              >
                <button
                  onClick={bulkArchiveSelected}
                  disabled={bulkBusy}
                  style={{
                    flex: 1,
                    padding: "10px",
                    borderRadius: 10,
                    border: "1px solid var(--border)",
                    background: "white",
                    color: "var(--text)",
                    fontWeight: 700,
                    fontSize: 13,
                    opacity: bulkBusy ? 0.6 : 1,
                    cursor: bulkBusy ? "not-allowed" : "pointer",
                    boxShadow: "var(--shadow)",
                  }}
                  title="Archive selected (hide from list)"
                >
                  Archive ({selectedCount})
                </button>
                <button
                  onClick={bulkDeleteSelected}
                  disabled={bulkBusy}
                  style={{
                    flex: 1,
                    padding: "10px",
                    borderRadius: 10,
                    border: "1px solid rgba(239, 68, 68, 0.2)",
                    background: "#fef2f2",
                    color: "var(--error)",
                    fontWeight: 700,
                    fontSize: 13,
                    opacity: bulkBusy ? 0.6 : 1,
                    cursor: bulkBusy ? "not-allowed" : "pointer",
                    boxShadow: "var(--shadow)",
                  }}
                  title="Delete selected (hide from list)"
                >
                  Delete
                </button>
                <button
                  onClick={clearSelection}
                  style={{
                    padding: "0 12px",
                    borderRadius: 10,
                    border: "none",
                    background: "transparent",
                    color: "var(--muted)",
                    fontWeight: 600,
                    fontSize: 13,
                    cursor: "pointer",
                  }}
                  title="Cancel selection"
                >
                  ✕
                </button>
              </div>
            ) : (
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  onClick={() => {
                    setSelectedJobId(null);
                    setMessage("");
                  }}
                  style={{
                    flex: 1,
                    textAlign: "left",
                    padding: "10px 12px",
                    borderRadius: 12,
                    border: "1px solid var(--border)",
                    background: "var(--panel)",
                    boxShadow: "var(--shadow)",
                    fontWeight: 600,
                    color: "var(--text)",
                  }}
                >
                  + New search
                </button>
                <button
                  onClick={() => setUploadModalOpen(true)}
                  style={{
                    padding: "10px 12px",
                    borderRadius: 12,
                    border: "1px solid var(--border)",
                    background: "var(--panel)",
                    boxShadow: "var(--shadow)",
                    fontWeight: 600,
                    color: "var(--muted)",
                  }}
                  title="Upload CSV to create a new list"
                >
                  ↑ Upload
                </button>
              </div>
            )}

            <div style={{ marginTop: 12, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div
                onClick={() => setShowActiveOnly(!showActiveOnly)}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  cursor: "pointer",
                  padding: "4px 8px",
                  borderRadius: 8,
                  marginLeft: "-8px",
                  transition: "background 0.1s",
                }}
                className="hover-bg"
              >
                <div
                  style={{
                    width: 16,
                    height: 16,
                    borderRadius: 4,
                    border: showActiveOnly ? "none" : "1px solid var(--muted)",
                    background: showActiveOnly ? "var(--accent)" : "transparent",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {showActiveOnly && (
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="4">
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  )}
                </div>
                <span style={{ fontSize: 13, color: "var(--text)", fontWeight: 500 }}>Show active only</span>
              </div>
            </div>
          </div>

          <div style={{ flex: 1, overflowY: "auto", padding: 12 }}>
            {jobs.length === 0 ? (
              <div
                style={{
                  padding: 14,
                  borderRadius: 14,
                  border: "1px solid var(--border)",
                  background: "var(--panel)",
                  color: "var(--muted)",
                  fontSize: 13,
                }}
              >
                No searches yet.
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {jobs
                  .filter((j) => !showActiveOnly || activity[j.id])
                  .map((j) => {
                    const isSelected = j.id === selectedJobId;
                    const act = activity[j.id];
                    const pauseBusy = !!pauseBusyByJobId[j.id];
                    const isChecked = !!selectedJobIds[j.id];
                    // Calculate dots
                    const scoutRunning = (act?.scouting_running || 0) > 0;
                    const scoutQueued = (act?.scouting_queued || 0) > 0;
                    const enrichRunning = (act?.enriching_running || 0) > 0;
                    const enrichQueued = (act?.enriching_queued || 0) > 0;
                    const hasDot = scoutRunning || scoutQueued || enrichRunning || enrichQueued;

                    return (
                      <button
                        key={j.id}
                        onClick={() => setSelectedJobId(j.id)}
                        style={{
                          width: "100%",
                          textAlign: "left",
                          padding: "10px 12px",
                          borderRadius: 12,
                          border: isSelected ? "1px solid rgba(99, 102, 241, 0.35)" : "1px solid transparent",
                          background: isSelected ? "rgba(99, 102, 241, 0.10)" : "transparent",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: 12,
                        }}
                      >
                        <div style={{ minWidth: 0, flex: 1 }}>
                          <div
                            style={{
                              fontWeight: 600,
                              color: "var(--text)",
                              whiteSpace: "nowrap",
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                            }}
                          >
                            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
                              <input
                                type="checkbox"
                                checked={isChecked}
                                onChange={(e) => {
                                  const v = e.target.checked;
                                  setSelectedJobIds((prev) => ({ ...prev, [j.id]: v }));
                                }}
                                onClick={(e) => {
                                  e.preventDefault();
                                  e.stopPropagation();
                                }}
                                style={{ accentColor: "var(--accent)", width: 14, height: 14, flex: "0 0 auto" }}
                                aria-label={`Select ${j.name}`}
                                title="Select job"
                              />
                              <span style={{ minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}>{j.name}</span>
                            </span>
                          </div>
                          <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 4 }}>
                            {hasDot && (
                              <button
                                type="button"
                                onClick={async (e) => {
                                  e.preventDefault();
                                  e.stopPropagation();
                                  if (pauseBusy) return;
                                  setPauseBusyByJobId((prev) => ({ ...prev, [j.id]: true }));
                                  try {
                                    const updated = await setJobPaused(j.id, !j.paused);
                                    // Refresh list + keep selected job in sync if needed
                                    await refreshJobs();
                                    if (selectedJobId === j.id) setSelectedJob(updated);
                                    showToast(updated.paused ? "Paused" : "Resumed", "success");
                                  } catch (err: any) {
                                    showToast(String(err?.message || err), "error");
                                  } finally {
                                    setPauseBusyByJobId((prev) => ({ ...prev, [j.id]: false }));
                                  }
                                }}
                                disabled={pauseBusy}
                                style={{
                                  padding: "2px 8px",
                                  borderRadius: 999,
                                  border: "1px solid var(--border)",
                                  background: j.paused ? "white" : "transparent",
                                  color: j.paused ? "var(--accent)" : "var(--muted)",
                                  fontSize: 11,
                                  fontWeight: 800,
                                  opacity: pauseBusy ? 0.6 : 1,
                                  cursor: pauseBusy ? "not-allowed" : "pointer",
                                }}
                                title={j.paused ? "Resume this job" : "Pause this job"}
                              >
                                {j.paused ? "Resume" : "Pause"}
                              </button>
                            )}

                            {hasDot && (
                              <div style={{ display: "flex", gap: 4 }}>
                                {(scoutRunning || scoutQueued) && (
                                  <div
                                    title={`Scouting: ${act?.scouting_running} running, ${act?.scouting_queued} queued`}
                                    className={scoutRunning && !j.paused ? "blinking-dot" : ""}
                                    style={{
                                      width: 7,
                                      height: 7,
                                      borderRadius: "50%",
                                      background: scoutRunning ? "#3b82f6" : "#94a3b8", // Blue running, Gray queued
                                    }}
                                  />
                                )}
                                {(enrichRunning || enrichQueued) && (
                                  <div
                                    title={`Enriching: ${act?.enriching_running} running, ${act?.enriching_queued} queued`}
                                    className={enrichRunning && !j.paused ? "blinking-dot" : ""}
                                    style={{
                                      width: 7,
                                      height: 7,
                                      borderRadius: "50%",
                                      background: enrichRunning ? "#a855f7" : "#94a3b8", // Purple running, Gray queued
                                    }}
                                  />
                                )}
                              </div>
                            )}
                          </div>
                          <div style={{ fontSize: 12, color: "var(--muted)" }}>
                            {new Date(j.created_at).toLocaleDateString()}
                          </div>
                        </div>

                        <div
                          style={{
                            flex: "0 0 auto",
                            minWidth: 44,
                            textAlign: "center",
                            padding: "4px 10px",
                            borderRadius: 999,
                            background: "var(--panel)",
                            border: "1px solid var(--border)",
                            color: "var(--text)",
                            fontWeight: 700,
                            fontSize: 12,
                          }}
                          title={`${j.profile_count} profiles`}
                        >
                          {j.profile_count}
                        </div>
                      </button>
                    );
                  })}
              </div>
            )}
          </div>
        </aside>
      )}

      {/* Main */}
      <main style={{ flex: 1, minWidth: 0, height: "100vh", display: "flex", flexDirection: "column" }}>
        <header
          style={{
            height: 56,
            borderBottom: "1px solid var(--border)",
            background: "var(--panel)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "0 16px",
            gap: 12,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
            {!sidebarOpen && (
              <button
                onClick={() => setSidebarOpen(true)}
                style={{
                  background: "transparent",
                  border: "1px solid var(--border)",
                  color: "var(--muted)",
                  borderRadius: 10,
                  padding: "6px 10px",
                  fontSize: 14,
                  flex: "0 0 auto",
                }}
                aria-label="Open sidebar"
                title="Open sidebar"
              >
                ☰
              </button>
            )}

            <div style={{ minWidth: 0 }}>
              <div
                style={{
                  fontWeight: 700,
                  color: "var(--text)",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  lineHeight: 1.1,
                }}
              >
                {selectedJob ? selectedJob.name : "New search"}
              </div>
              <div style={{ fontSize: 12, color: "var(--muted)" }}>
                {selectedJob
                  ? `${selectedJob.profile_count} profiles • ${selectedJob.phone_count} phones`
                  : `${jobs.length} searches`}
              </div>
            </div>
          </div>

          {activeJobsCount > 0 && (
            <div
              style={{
                background: "var(--accent-soft)",
                color: "var(--accent)",
                padding: "6px 12px",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 700,
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <div
                className="blinking-dot"
                style={{ width: 6, height: 6, borderRadius: "50%", background: "currentColor" }}
              />
              Active: {activeJobsCount} job{activeJobsCount !== 1 ? "s" : ""}
            </div>
          )}

          {selectedJobId && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, flex: "0 0 auto" }}>
              <button
                onClick={startEnrich}
                disabled={!selectedJob || selectedJob.profile_count === 0 || isEnriching}
                style={{
                  background:
                    !selectedJob || selectedJob.profile_count === 0 || isEnriching ? "var(--bg)" : "white",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  fontWeight: 700,
                  opacity: !selectedJob || selectedJob.profile_count === 0 || isEnriching ? 0.6 : 1,
                }}
                title="Find phones for this job"
              >
                {isEnriching ? "Finding phones..." : "Find phones"}
              </button>

              <button
                onClick={startUpload}
                disabled={!selectedJob || isUploading || isEnriching}
                style={{
                  background: !selectedJob || isUploading || isEnriching ? "var(--bg)" : "white",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  fontWeight: 700,
                  opacity: !selectedJob || isUploading || isEnriching ? 0.6 : 1,
                }}
                title="Upload profiles CSV into this job"
              >
                {isUploading ? "Uploading..." : "Upload CSV"}
              </button>

              <input
                ref={uploadInputRef}
                type="file"
                accept=".csv,text/csv"
                style={{ display: "none" }}
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  // Clear immediately to allow selecting the same file twice.
                  e.target.value = "";
                  if (!f) return;
                  handleUploadFile(f).catch(() => {});
                }}
              />

              <button
                onClick={openProfiles}
                disabled={!selectedJob || selectedJob.profile_count === 0}
                style={{
                  background: !selectedJob || selectedJob.profile_count === 0 ? "var(--bg)" : "white",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  fontWeight: 700,
                  opacity: !selectedJob || selectedJob.profile_count === 0 ? 0.6 : 1,
                  cursor: !selectedJob || selectedJob.profile_count === 0 ? "not-allowed" : "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                }}
                title="View profiles for this job"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
                  <path
                    d="M16 11c1.66 0 3-1.34 3-3S17.66 5 16 5s-3 1.34-3 3 1.34 3 3 3Zm-8 0c1.66 0 3-1.34 3-3S9.66 5 8 5 5 6.34 5 8s1.34 3 3 3Zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5C15 14.17 10.33 13 8 13Zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.95 1.97 3.45V19h6v-2.5C24 14.17 19.33 13 16 13Z"
                    fill="currentColor"
                  />
                </svg>
                Profiles
              </button>

              <a
                href={selectedJobId ? getBasicProfilesDownloadUrl(selectedJobId, chainSource) : "#"}
                download
                style={{
                  background: !selectedJob || selectedJob.profile_count === 0 ? "var(--bg)" : "white",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  fontWeight: 700,
                  textDecoration: "none",
                  opacity: !selectedJob || selectedJob.profile_count === 0 ? 0.6 : 1,
                  pointerEvents: !selectedJob || selectedJob.profile_count === 0 ? "none" : "auto",
                }}
                title="Download basic profiles CSV (name, location, title, phone_numbers)"
              >
                Download
              </a>

              <button
                onClick={() => setSelectedJobId(null)}
                style={{
                  background: "transparent",
                  border: "1px solid var(--border)",
                  color: "var(--muted)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  fontSize: 13,
                  flex: "0 0 auto",
                }}
                title="Start a new search"
              >
                New search
              </button>
            </div>
          )}
        </header>

        <div style={{ flex: 1, overflow: "hidden", padding: 24, display: "flex", justifyContent: "center" }}>
          <div className="mainSplit">
            {/* Left: chat + input */}
            <div
              className="mainLeft"
              style={{
                height: "100%",
                minHeight: 0,
                background: "var(--panel)",
                border: "1px solid var(--border)",
                borderRadius: 16,
                boxShadow: "var(--shadow)",
                display: "flex",
                flexDirection: "column",
                overflow: "hidden",
              }}
            >
              {/* Messages */}
              <div style={{ flex: 1, padding: 24, overflowY: "auto" }}>
                {chat.length === 0 ? (
                  <div
                    style={{
                      height: "100%",
                      display: "flex",
                      flexDirection: "column",
                      alignItems: "center",
                      justifyContent: "center",
                      textAlign: "center",
                      gap: 16,
                    }}
                  >
                    <div
                      style={{
                        width: 64,
                        height: 64,
                        borderRadius: 16,
                        background: "linear-gradient(135deg, #6366f1, #8b5cf6)",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        fontSize: 28,
                      }}
                    >
                      👋
                    </div>
                    <div>
                      <h2 style={{ margin: 0, fontWeight: 700, fontSize: 20, color: "var(--text)" }}>
                        Who do you want to find?
                      </h2>
                      <p style={{ margin: "8px 0 0", color: "var(--muted)", fontSize: 15 }}>
                        Job title + location is enough to start.
                      </p>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 8 }}>
                      {[
                        "Find dentists in Los Angeles and San Diego",
                        "Software engineers in Austin, Seattle, and Portland",
                        "Real estate agents in Miami and Tampa",
                      ].map((ex) => (
                        <button
                          key={ex}
                          onClick={() => setMessage(ex)}
                          style={{
                            padding: "10px 16px",
                            borderRadius: 10,
                            border: "1px solid var(--border)",
                            background: "var(--panel)",
                            color: "var(--text)",
                            fontSize: 14,
                            boxShadow: "var(--shadow)",
                          }}
                        >
                          "{ex}"
                        </button>
                      ))}
                    </div>

                    <div
                      style={{
                        marginTop: 24,
                        paddingTop: 24,
                        borderTop: "1px solid var(--border)",
                        width: "100%",
                        maxWidth: 360,
                      }}
                    >
                      <p style={{ margin: "0 0 12px", color: "var(--muted)", fontSize: 13 }}>
                        Or upload your own list
                      </p>
                      <button
                        onClick={() => setUploadModalOpen(true)}
                        style={{
                          padding: "12px 20px",
                          borderRadius: 10,
                          border: "1px solid var(--border)",
                          background: "white",
                          color: "var(--text)",
                          fontSize: 14,
                          fontWeight: 700,
                          boxShadow: "var(--shadow)",
                          display: "inline-flex",
                          alignItems: "center",
                          gap: 8,
                        }}
                      >
                        <span>📄</span> Upload CSV
                      </button>
                    </div>
                  </div>
                ) : (
                  chat.map((m) => (
                    <div
                      key={m.id}
                      style={{
                        marginBottom: 20,
                        display: "flex",
                        flexDirection: "column",
                        alignItems: m.role === "user" ? "flex-end" : "flex-start",
                      }}
                    >
                      <div
                        style={{
                          maxWidth: "85%",
                          padding: "12px 16px",
                          borderRadius: 14,
                          background:
                            m.role === "user"
                              ? "linear-gradient(135deg, #6366f1, #8b5cf6)"
                              : "#e0e7ff",
                          color: m.role === "user" ? "white" : "var(--text)",
                          fontSize: 14,
                          lineHeight: 1.5,
                          whiteSpace: "pre-wrap",
                        }}
                      >
                        {m.content}
                      </div>
                    </div>
                  ))
                )}
                <div ref={chatEndRef} />
              </div>

              {/* Input */}
              <div style={{ borderTop: "1px solid var(--border)", padding: 16, background: "var(--panel)" }}>
                <div style={{ display: "flex", gap: 12 }}>
                  <input
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    placeholder={selectedJobId ? "Add more titles or locations..." : "Describe who you want to find..."}
                    onKeyDown={(e) => e.key === "Enter" && send()}
                    style={{
                      flex: 1,
                      background: "var(--bg)",
                      border: "1px solid var(--border)",
                      borderRadius: 12,
                      padding: "12px 16px",
                      color: "var(--text)",
                      fontSize: 15,
                    }}
                    disabled={sending}
                  />
                  <button
                    onClick={send}
                    disabled={!message.trim() || sending}
                    style={{
                      background: message.trim() ? "var(--accent)" : "var(--border)",
                      border: "none",
                      color: message.trim() ? "white" : "var(--muted)",
                      padding: "12px 20px",
                      borderRadius: 12,
                      fontWeight: 700,
                      fontSize: 15,
                      cursor: message.trim() ? "pointer" : "not-allowed",
                      flex: "0 0 auto",
                    }}
                  >
                    {sending ? "..." : "Send"}
                  </button>
                </div>
              </div>
            </div>

            {/* Right: boolean combos */}
            {hasVariants && (
              <div
                className="mainRight"
                style={{
                  height: "100%",
                  minHeight: 0,
                  background: "var(--panel)",
                  border: "1px solid var(--border)",
                  borderRadius: 16,
                  boxShadow: "var(--shadow)",
                  overflow: "hidden",
                  display: "flex",
                  flexDirection: "column",
                }}
              >
                <div style={{ padding: 16, borderBottom: "1px solid var(--border)" }}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
                    <div style={{ fontSize: 13, color: "var(--muted)" }}>
                      {selectedTitleCount} × {selectedLocationCount} ={" "}
                      <span style={{ fontWeight: 800, color: "var(--text)" }}>{comboCount}</span>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, flex: "0 0 auto" }}>
                      {selectedJobId && jobRuns.length > 0 && (
                        <button
                          onClick={() => setRunsOpen(true)}
                          style={{
                            padding: "8px 10px",
                            borderRadius: 10,
                            border: "1px solid var(--border)",
                            background: "white",
                            color: "var(--text)",
                            fontWeight: 800,
                            fontSize: 12,
                            cursor: "pointer",
                          }}
                          title="View runs"
                        >
                          Runs
                        </button>
                      )}
                      <button
                        onClick={runSelected}
                        disabled={!selectedJobId || comboCount === 0 || isRunning}
                        style={{
                          padding: "8px 12px",
                          borderRadius: 10,
                          border: "none",
                          background: comboCount > 0 && !isRunning ? "var(--accent)" : "var(--border)",
                          color: comboCount > 0 && !isRunning ? "white" : "var(--muted)",
                          fontWeight: 800,
                          fontSize: 12,
                          cursor: comboCount > 0 && !isRunning ? "pointer" : "not-allowed",
                        }}
                        title="Run selected title/location combos"
                      >
                        {isRunning ? "Running..." : "Run"}
                      </button>
                    </div>
                  </div>
                </div>

                <div style={{ padding: 16, overflowY: "auto" }}>
                  {/* Pending queue */}
                  {selectedJobId && pendingQueue && (
                    <div style={{ marginBottom: 16 }}>
                      <div
                        style={{
                          background: "var(--bg)",
                          border: "1px solid var(--border)",
                          borderRadius: 12,
                          padding: "10px 12px",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10 }}>
                          <div style={{ fontSize: 13, fontWeight: 800, color: "var(--text)" }}>Pending queue</div>
                          {pendingQueue.paused && (
                            <div style={{ fontSize: 11, fontWeight: 800, color: "var(--muted)" }} title="Paused jobs won't start new work">
                              Paused
                            </div>
                          )}
                        </div>

                        <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 8 }}>
                          <div style={{ fontSize: 12, color: "var(--muted)" }}>
                            Scouting queued: <span style={{ color: "var(--text)", fontWeight: 800 }}>{pendingQueue.scouting_queued_count}</span>
                          </div>

                          {pendingQueue.scouting_queued.length > 0 && (
                            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                              {pendingQueue.scouting_queued.slice(0, 8).map((r) => {
                                const titleLabel = titleLabelById.get(r.title_variant_id) || r.title_variant_id;
                                const locLabel = locationLabelById.get(r.location_variant_id) || r.location_variant_id;
                                return (
                                  <div key={r.id} style={{ fontSize: 12, color: "var(--text)" }} title={`Queued scouting run ${r.id}`}>
                                    <span style={{ color: "var(--muted)" }}>•</span> {titleLabel} <span style={{ color: "var(--muted)" }}>•</span>{" "}
                                    {locLabel}
                                  </div>
                                );
                              })}
                              {pendingQueue.scouting_queued_count > pendingQueue.scouting_queued.length && (
                                <div style={{ fontSize: 12, color: "var(--muted)" }}>
                                  +{pendingQueue.scouting_queued_count - pendingQueue.scouting_queued.length} more
                                </div>
                              )}
                            </div>
                          )}

                          <div style={{ fontSize: 12, color: "var(--muted)" }}>
                            Phone lookups queued:{" "}
                            <span style={{ color: "var(--text)", fontWeight: 800 }}>{pendingQueue.enrichment_queued_count}</span>
                          </div>

                          {pendingQueue.enrichment_queued_by_source.length > 0 && (
                            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                              {pendingQueue.enrichment_queued_by_source.map((x) => (
                                <div
                                  key={x.source}
                                  style={{
                                    fontSize: 11,
                                    fontWeight: 800,
                                    padding: "4px 8px",
                                    borderRadius: 999,
                                    border: "1px solid var(--border)",
                                    background: "white",
                                    color: "var(--text)",
                                  }}
                                  title="Queued by source"
                                >
                                  {x.source}: {x.queued}
                                </div>
                              ))}
                            </div>
                          )}

                          {pendingQueue.enrichment_queued_sample.length > 0 && (
                            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                              {pendingQueue.enrichment_queued_sample.slice(0, 5).map((p) => (
                                <div key={p.enrichment_id} style={{ fontSize: 12, color: "var(--text)" }}>
                                  <span style={{ color: "var(--muted)" }}>•</span> {p.name}{" "}
                                  <span style={{ color: "var(--muted)" }}>({[p.city, p.state].filter(Boolean).join(", ")})</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Variants Accordion */}
                  {(titleVariants.length > 0 || locationVariants.length > 0) && (
                    <div style={{ marginBottom: 16 }}>
                      <button
                        onClick={() => setVariantsExpanded(!variantsExpanded)}
                        style={{
                          width: "100%",
                          background: "var(--bg)",
                          border: "1px solid var(--border)",
                          borderRadius: 12,
                          padding: "10px 12px",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          cursor: "pointer",
                          marginBottom: variantsExpanded ? 8 : 0,
                          fontSize: 13,
                          fontWeight: 700,
                          color: "var(--text)",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ fontSize: 16 }}>{variantsExpanded ? "▾" : "▸"}</span>
                          <span>
                            {variantsExpanded
                              ? "Variants"
                              : `${selectedTitleCount} Title × ${selectedLocationCount} Location`}
                          </span>
                        </div>
                        {!variantsExpanded && (
                          <div style={{ fontSize: 12, color: "var(--muted)", fontWeight: 500 }}>
                            {comboCount} combos
                          </div>
                        )}
                      </button>

                      {variantsExpanded && (
                        <div className="accordion-content">
                          {/* Title Variants */}
                          {titleVariants.length > 0 && (
                            <div>
                              <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 6 }}>Titles</div>
                              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                                {titleVariants.map((v) => (
                                  <button
                                    key={v.id}
                                    onClick={() => handleToggleTitle(v)}
                                    style={{
                                      padding: "7px 12px",
                                      borderRadius: 999,
                                      border: v.selected
                                        ? "1px solid rgba(99, 102, 241, 0.45)"
                                        : "1px solid var(--border)",
                                      background: v.selected ? "var(--accent-soft)" : "var(--bg)",
                                      color: v.selected ? "var(--accent)" : "var(--text)",
                                      fontSize: 12,
                                      fontWeight: v.selected ? 700 : 500,
                                    }}
                                    title={v.boolean_text}
                                  >
                                    {(v.boolean_text || v.entities.join(" + ")).trim()}
                                  </button>
                                ))}
                              </div>
                            </div>
                          )}

                          {/* Location Variants */}
                          {locationVariants.length > 0 && (
                            <div style={{ marginTop: 12 }}>
                              <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 6 }}>Locations</div>
                              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                                {locationVariants.map((v) => (
                                  <button
                                    key={v.id}
                                    onClick={() => handleToggleLocation(v)}
                                    style={{
                                      padding: "7px 12px",
                                      borderRadius: 999,
                                      border: v.selected
                                        ? "1px solid rgba(99, 102, 241, 0.45)"
                                        : "1px solid var(--border)",
                                      background: v.selected ? "var(--accent-soft)" : "var(--bg)",
                                      color: v.selected ? "var(--accent)" : "var(--text)",
                                      fontSize: 12,
                                      fontWeight: v.selected ? 700 : 500,
                                    }}
                                    title={v.boolean_text}
                                  >
                                    {(v.boolean_text || v.entities.join(", ")).trim()}
                                  </button>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Active Runs Accordion */}
                  {activeRuns.length > 0 && (
                    <div>
                      <button
                        onClick={() => setActiveRunsExpanded(!activeRunsExpanded)}
                        style={{
                          width: "100%",
                          background: "var(--bg)",
                          border: "1px solid var(--border)",
                          borderRadius: 12,
                          padding: "10px 12px",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          cursor: "pointer",
                          marginBottom: activeRunsExpanded ? 8 : 0,
                          fontSize: 13,
                          fontWeight: 700,
                          color: "var(--text)",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ fontSize: 16 }}>{activeRunsExpanded ? "▾" : "▸"}</span>
                          <span>
                            {activeRunsExpanded
                              ? "Active Runs"
                              : `Running ${activeRuns.filter((r) => r.status === "running").length} / ${
                                  activeRuns.length
                                }`}
                          </span>
                        </div>
                        {!activeRunsExpanded && activeRuns.some((r) => r.status === "running") && (
                          <div style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--accent)" }} />
                        )}
                      </button>

                      {activeRunsExpanded && (
                        <div className="accordion-content" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                          {activeRuns.map((run) => (
                            <div
                              key={run.id}
                              style={{
                                padding: "8px 10px",
                                background: "var(--bg)",
                                border: "1px solid var(--border)",
                                borderRadius: 12,
                                fontSize: 12,
                              }}
                            >
                              <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                                <span style={{ color: "var(--muted)" }}>
                                  {run.pages_completed}/{run.pages_total} pages
                                </span>
                                <span
                                  style={{
                                    fontWeight: 800,
                                    color:
                                      run.status === "completed"
                                        ? "var(--success)"
                                        : run.status === "failed"
                                        ? "var(--error)"
                                        : "var(--accent)",
                                  }}
                                >
                                  {run.status}
                                </span>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </main>

      {/* Runs modal (combos history) */}
      {runsOpen && selectedJobId && (
        <div
          onClick={() => setRunsOpen(false)}
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            zIndex: 200,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 16,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: "100%",
              maxWidth: 980,
              maxHeight: "85vh",
              overflow: "hidden",
              background: "var(--panel)",
              border: "1px solid var(--border)",
              borderRadius: 16,
              boxShadow: "var(--shadow)",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div
              style={{
                padding: 14,
                borderBottom: "1px solid var(--border)",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 10,
              }}
            >
              <div style={{ fontWeight: 900, fontSize: 14, color: "var(--text)" }}>Runs</div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                {jobRuns.some((r) => {
                  if (r.status !== "running") return false;
                  if (!r.started_at) return false;
                  const startedMs = parseDbTimestampUtc(r.started_at);
                  if (!startedMs) return false;
                  const ageMs = Date.now() - startedMs;
                  return ageMs > 30 * 60 * 1000 && (r.pages_completed || 0) === 0;
                }) && (
                  <button
                    onClick={doResetStuck}
                    style={{
                      background: "white",
                      border: "1px solid var(--border)",
                      color: "var(--text)",
                      borderRadius: 10,
                      padding: "6px 10px",
                      fontSize: 12,
                      fontWeight: 900,
                      cursor: "pointer",
                    }}
                    title="If a run is stuck as running (worker restarted), reset it and enqueue the next queued run."
                  >
                    Reset stuck
                  </button>
                )}
                <button
                  onClick={() => setRunsOpen(false)}
                  style={{
                    background: "white",
                    border: "1px solid var(--border)",
                    color: "var(--text)",
                    borderRadius: 10,
                    padding: "6px 10px",
                    fontSize: 12,
                    fontWeight: 900,
                    cursor: "pointer",
                  }}
                  title="Close"
                >
                  Close
                </button>
              </div>
            </div>

            <div style={{ padding: 14, overflow: "auto" }}>
              <div style={{ border: "1px solid var(--border)", borderRadius: 12, overflowX: "auto" }}>
                <table style={{ width: "100%", minWidth: 980, borderCollapse: "separate", borderSpacing: 0, fontSize: 12 }}>
                  <thead>
                    <tr style={{ background: "var(--bg)" }}>
                      {["Title", "Location", "Status", "Pages", "Added", "Dropped", "Error", "Action"].map((h, idx) => (
                        <th
                          key={h}
                          style={{
                            textAlign: "left",
                            padding: "10px 12px",
                            borderBottom: "1px solid var(--border)",
                            color: "var(--muted)",
                            fontWeight: 900,
                            whiteSpace: "nowrap",
                            ...(idx === 7
                              ? {
                                  position: "sticky",
                                  right: 0,
                                  background: "var(--bg)",
                                  zIndex: 3,
                                  borderLeft: "1px solid var(--border)",
                                }
                              : {}),
                          }}
                        >
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {jobRuns.slice(0, 200).map((r) => {
                      const titleLabel = titleLabelById.get(r.title_variant_id) || r.title_variant_id;
                      const locLabel = locationLabelById.get(r.location_variant_id) || r.location_variant_id;
                      const inFlight = r.status === "queued" || r.status === "running";
                      return (
                        <tr key={r.id}>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                            <div
                              style={{ maxWidth: 320, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                              title={titleLabel}
                            >
                              {titleLabel}
                            </div>
                          </td>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                            <div
                              style={{ maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                              title={locLabel}
                            >
                              {locLabel}
                            </div>
                          </td>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>
                            <span style={{ fontWeight: 900 }}>{r.status}</span>
                          </td>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)", color: "var(--muted)" }}>
                            {r.pages_completed}/{r.pages_total}
                          </td>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>{r.added_count}</td>
                          <td style={{ padding: "10px 12px", borderBottom: "1px solid var(--border)" }}>{r.dropped_count}</td>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: "1px solid var(--border)",
                              color: r.last_error ? "var(--error)" : "var(--muted)",
                            }}
                          >
                            <div
                              style={{ maxWidth: 380, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                              title={r.last_error || ""}
                            >
                              {r.last_error || "—"}
                            </div>
                          </td>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: "1px solid var(--border)",
                              position: "sticky",
                              right: 0,
                              background: "var(--panel)",
                              borderLeft: "1px solid var(--border)",
                              zIndex: 2,
                            }}
                          >
                            <button
                              onClick={() => doRerun(r.id)}
                              disabled={inFlight}
                              style={{
                                background: "white",
                                border: "1px solid var(--border)",
                                color: "var(--text)",
                                borderRadius: 10,
                                padding: "6px 10px",
                                fontSize: 12,
                                fontWeight: 900,
                                opacity: inFlight ? 0.5 : 1,
                                cursor: inFlight ? "not-allowed" : "pointer",
                              }}
                              title={inFlight ? "Wait for in-flight runs to finish" : "Rerun this combo"}
                            >
                              Rerun
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                    {jobRuns.length > 200 && (
                      <tr>
                        <td colSpan={8} style={{ padding: 12, color: "var(--muted)" }}>
                          Showing latest 200 runs.
                        </td>
                      </tr>
                    )}
                    {jobRuns.length === 0 && (
                      <tr>
                        <td colSpan={8} style={{ padding: 12, color: "var(--muted)" }}>
                          No runs yet.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      )}

      <style jsx global>{`
        .mainSplit {
          width: 100%;
          max-width: 1100px;
          height: 100%;
          min-height: 0;
          display: flex;
          gap: 16px;
          align-items: stretch;
        }
        .mainLeft {
          flex: 1 1 0;
          min-width: 0;
        }
        .mainRight {
          width: 360px;
          flex: 0 0 360px;
        }
        @media (max-width: 980px) {
          .mainSplit {
            flex-direction: column;
          }
          .mainRight {
            width: 100%;
            flex: 0 0 auto;
          }
        }
        .hover-bg:hover {
          background: rgba(0,0,0,0.04);
        }
        @keyframes slideDown {
          from {
            opacity: 0;
            transform: translateY(-10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        .accordion-content {
          animation: slideDown 0.2s ease-out forwards;
        }
        .blinking-dot {
          animation: blink 1.5s infinite;
        }
        @keyframes blink {
          0% { opacity: 1; }
          50% { opacity: 0.4; }
          100% { opacity: 1; }
        }
      `}</style>
    </div>
  );
}
