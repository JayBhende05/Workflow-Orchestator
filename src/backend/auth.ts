import { timingSafeEqual } from "node:crypto"
import type { NextFunction, Request, Response } from "express"

// Call once at startup so the server never runs unprotected by accident
export function assertApiKeyConfigured(): void {
  if (!process.env.API_KEY) {
    console.error("❌ API_KEY is not set. Add API_KEY=<secret> to .env before starting the server.")
    process.exit(1)
  }
}

function keysMatch(provided: string, expected: string): boolean {
  const a = Buffer.from(provided)
  const b = Buffer.from(expected)
  return a.length === b.length && timingSafeEqual(a, b)
}

// Express middleware: runs before every route and ends the request with 401 on a bad key
export function requireApiKey(req: Request, res: Response, next: NextFunction): void {
  const expected = process.env.API_KEY
  const provided = req.header("x-api-key")

  if (!expected || !provided || !keysMatch(provided, expected)) {
    res.status(401).json({ error: "Unauthorized" })
    return
  }

  next()
}
