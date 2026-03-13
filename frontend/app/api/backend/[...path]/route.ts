import { NextRequest } from "next/server";

export const runtime = "nodejs";

function backendBaseUrl(): string {
  const b =
    process.env.BACKEND_URL ||
    process.env.NEXT_PUBLIC_BACKEND_URL ||
    "http://127.0.0.1:8000";
  return String(b).replace(/\/+$/, "");
}

function adminToken(): string {
  return (process.env.ADMIN_API_TOKEN || "").trim();
}

function isHopByHopHeader(name: string): boolean {
  const n = name.toLowerCase();
  return (
    n === "connection" ||
    n === "keep-alive" ||
    n === "proxy-authenticate" ||
    n === "proxy-authorization" ||
    n === "te" ||
    n === "trailer" ||
    n === "transfer-encoding" ||
    n === "upgrade" ||
    n === "host"
  );
}

async function forward(req: NextRequest, pathParts: string[]): Promise<Response> {
  const base = backendBaseUrl();
  const targetPath = "/" + pathParts.map(encodeURIComponent).join("/");
  const url = new URL(base + targetPath);
  url.search = req.nextUrl.search;

  const headers = new Headers();
  req.headers.forEach((value, key) => {
    if (isHopByHopHeader(key)) return;
    headers.set(key, value);
  });

  const token = adminToken();
  if (token) {
    headers.set("authorization", `Bearer ${token}`);
  }

  // Let fetch compute content-length.
  headers.delete("content-length");

  const method = req.method.toUpperCase();
  const body =
    method === "GET" || method === "HEAD" ? undefined : await req.arrayBuffer();

  const upstream = await fetch(url.toString(), {
    method,
    headers,
    body,
    redirect: "manual",
  });

  const resHeaders = new Headers();
  upstream.headers.forEach((value, key) => {
    if (isHopByHopHeader(key)) return;
    resHeaders.set(key, value);
  });

  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: resHeaders,
  });
}

export async function GET(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  return forward(req, path || []);
}

export async function POST(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  return forward(req, path || []);
}

export async function PATCH(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  return forward(req, path || []);
}

export async function PUT(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  return forward(req, path || []);
}

export async function DELETE(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  return forward(req, path || []);
}

