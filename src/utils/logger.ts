// Small tagged logger so the server output reads as a timeline that can be screenshotted.
// Format:  12:00:01.234  TAG       message
// Set NO_COLOR=1 to turn colors off.

const useColor = !process.env.NO_COLOR

const COLORS = {
  reset: "\x1b[0m",
  dim: "\x1b[2m",
  cyan: "\x1b[36m",
  blue: "\x1b[34m",
  yellow: "\x1b[33m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  magenta: "\x1b[35m",
  gray: "\x1b[90m",
}

type Tag = "HTTP" | "WORKFLOW" | "QUEUE" | "DISPATCH" | "POD" | "STEP" | "SERVER"

const TAG_COLOR: Record<Tag, string> = {
  HTTP: COLORS.gray,
  WORKFLOW: COLORS.magenta,
  QUEUE: COLORS.blue,
  DISPATCH: COLORS.cyan,
  POD: COLORS.yellow,
  STEP: COLORS.green,
  SERVER: COLORS.gray,
}

function paint(color: string, text: string): string {
  return useColor ? `${color}${text}${COLORS.reset}` : text
}

function timestamp(): string {
  return new Date().toISOString().slice(11, 23)
}

function write(tag: Tag, message: string, color?: string, error = false): void {
  const line = `${paint(COLORS.dim, timestamp())}  ${paint(TAG_COLOR[tag], tag.padEnd(8))}  ${
    color ? paint(color, message) : message
  }`
  if (error) console.error(line)
  else console.log(line)
}

export const log = {
  info: (tag: Tag, message: string) => write(tag, message),
  success: (tag: Tag, message: string) => write(tag, message, COLORS.green),
  warn: (tag: Tag, message: string) => write(tag, message, COLORS.yellow),
  error: (tag: Tag, message: string) => write(tag, message, COLORS.red, true),
  // A visible divider line, used to separate one workflow's run from the next
  banner: (message: string) => {
    console.log(paint(COLORS.dim, "─".repeat(72)))
    console.log(paint(COLORS.magenta, message))
    console.log(paint(COLORS.dim, "─".repeat(72)))
  },
}
