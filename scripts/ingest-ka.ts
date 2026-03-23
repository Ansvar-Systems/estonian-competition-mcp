/**
 * Ingestion crawler for the Konkurentsiamet (Estonian Competition Authority) MCP server.
 *
 * Scrapes competition enforcement decisions and merger control decisions from
 * konkurentsiamet.ee and populates the SQLite database.
 *
 * Data sources:
 *   - Competition cases (juhtumid):
 *     https://www.konkurentsiamet.ee/konkurentsijarelevalve-ja-koondumised/konkurentsijarelevalve/juhtumid
 *     Single-page HTML table with ~274 entries (2004-present). Each row links to
 *     a PDF decision document via /media/{ID}/download or /sites/default/files/...
 *
 *   - Merger notifications and decisions (koondumiste teated ja otsused):
 *     https://www.konkurentsiamet.ee/konkurentsijarelevalve-ja-koondumised/koondumised/koondumiste-teated-ja-otsused
 *     Single-page HTML table with ~450 entries (2001-present). Each row contains
 *     notification date, parties summary (linked PDF), AT publication date, and
 *     decision (linked PDF).
 *
 * Table columns (competition cases):
 *   - Kuupäev          (date, dd.MM.yyyy)
 *   - Nr               (case number, e.g. 5-5/2023-022)
 *   - Pealkiri         (title, clickable link to PDF)
 *   - Tegevusala       (sector/activity area in Estonian)
 *
 * Table columns (mergers):
 *   - Teate esitamise kuupäev                   (notification date)
 *   - Koondumise osalised ja lühikokkuvõte      (parties and summary, linked PDF)
 *   - AT-s avaldamise alustamise kuupäev        (AT publication start, linked PDF with AT ref)
 *   - AT-s avaldamise lõpetamise kuupäev        (AT publication end, linked PDF with AT ref)
 *   - Otsus                                     (decision, linked PDF with case number)
 *
 * Usage:
 *   npx tsx scripts/ingest-ka.ts
 *   npx tsx scripts/ingest-ka.ts --dry-run
 *   npx tsx scripts/ingest-ka.ts --resume
 *   npx tsx scripts/ingest-ka.ts --force
 *   npx tsx scripts/ingest-ka.ts --max-pages 5
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["ECA_DB_PATH"] ?? "data/eca.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.konkurentsiamet.ee";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarKACrawler/1.0 (+https://github.com/Ansvar-Systems/estonian-competition-mcp)";

/** Listing pages to crawl. */
const LISTING_PAGES = {
  decisions: {
    url: `${BASE_URL}/konkurentsijarelevalve-ja-koondumised/konkurentsijarelevalve/juhtumid`,
    id: "juhtumid",
  },
  mergers: {
    url: `${BASE_URL}/konkurentsijarelevalve-ja-koondumised/koondumised/koondumiste-teated-ja-otsused`,
    id: "koondumised",
  },
} as const;

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPagesOverride = maxPagesArg ? parseInt(maxPagesArg, 10) : null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedCaseNumbers: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface DecisionRow {
  date: string;
  case_number: string;
  title: string;
  href: string;
  sector: string;
}

interface MergerRow {
  date: string;
  parties_text: string;
  parties_href: string | null;
  at_ref: string | null;
  decision_href: string | null;
  decision_text: string | null;
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "et,en;q=0.5",
        },
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

/**
 * Fetch a PDF and extract readable text content.
 *
 * Konkurentsiamet publishes decisions as PDFs. We download the raw bytes and
 * extract text by scanning for PDF text operators (Tj, TJ, ').  This produces
 * imperfect but usable plaintext without requiring a native PDF library.
 */
async function fetchPdfText(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "application/pdf,*/*;q=0.8",
          "Accept-Language": "et,en;q=0.5",
        },
        signal: AbortSignal.timeout(60_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] PDF HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] PDF HTTP ${response.status} for ${url}`);
        return null;
      }

      const buffer = Buffer.from(await response.arrayBuffer());
      return extractTextFromPdf(buffer);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] PDF fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

/**
 * Lightweight PDF text extraction.
 *
 * Scans the raw PDF byte stream for text-showing operators (Tj, TJ, ')
 * and decodes parenthesised string operands. This covers the majority of
 * Konkurentsiamet PDFs, which use simple text encoding (WinAnsi / Latin-1).
 *
 * Limitations: Does not handle CIDFont/ToUnicode CMap streams, so CJK or
 * heavily-subset fonts may produce garbled output. For Estonian (Latin-based)
 * text this works well.
 */
function extractTextFromPdf(buffer: Buffer): string {
  const raw = buffer.toString("latin1");
  const chunks: string[] = [];

  // Match parenthesised strings before Tj or ' operators, and array
  // operands before TJ.  The regex is intentionally broad — false positives
  // are filtered by the minimum-length check below.
  const tjPattern = /\(([^)]*)\)\s*Tj/g;
  const tjArrayPattern = /\[([^\]]*)\]\s*TJ/g;
  const tickPattern = /\(([^)]*)\)\s*'/g;

  let m: RegExpExecArray | null;

  while ((m = tjPattern.exec(raw)) !== null) {
    const decoded = decodePdfString(m[1]!);
    if (decoded.length > 0) chunks.push(decoded);
  }

  while ((m = tjArrayPattern.exec(raw)) !== null) {
    // TJ arrays alternate strings and kerning offsets: [(H) 20 (ello)]
    const inner = m[1]!;
    const parts: string[] = [];
    const strPattern = /\(([^)]*)\)/g;
    let s: RegExpExecArray | null;
    while ((s = strPattern.exec(inner)) !== null) {
      parts.push(decodePdfString(s[1]!));
    }
    if (parts.length > 0) chunks.push(parts.join(""));
  }

  while ((m = tickPattern.exec(raw)) !== null) {
    const decoded = decodePdfString(m[1]!);
    if (decoded.length > 0) chunks.push(decoded);
  }

  // Join chunks, collapse whitespace runs, and trim.
  const text = chunks
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();

  return text;
}

/** Decode a PDF parenthesised string (handles octal escapes and common escape sequences). */
function decodePdfString(s: string): string {
  return s.replace(/\\(\d{3})|\\(.)/g, (_match, octal?: string, ch?: string) => {
    if (octal) return String.fromCharCode(parseInt(octal, 8));
    switch (ch) {
      case "n":
        return "\n";
      case "r":
        return "\r";
      case "t":
        return "\t";
      case "b":
        return "\b";
      case "f":
        return "\f";
      case "(":
        return "(";
      case ")":
        return ")";
      case "\\":
        return "\\";
      default:
        return ch ?? "";
    }
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedCaseNumbers: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Estonian date parsing
// ---------------------------------------------------------------------------

/**
 * Parse an Estonian date string (dd.MM.yyyy) to ISO format (yyyy-MM-dd).
 *
 * Also handles already-ISO dates and Estonian textual months.
 */
function parseEstonianDate(raw: string): string | null {
  if (!raw) return null;

  // dd.MM.yyyy (most common on Konkurentsiamet pages)
  const dotMatch = raw.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (dotMatch) {
    const [, day, month, year] = dotMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Estonian textual months: "2. detsember 2025"
  const estonianMonths: Record<string, string> = {
    jaanuar: "01",
    jaanuaril: "01",
    veebruar: "02",
    veebruaril: "02",
    märts: "03",
    märtsil: "03",
    aprill: "04",
    aprillil: "04",
    mai: "05",
    mail: "05",
    juuni: "06",
    juunil: "06",
    juuli: "07",
    juulil: "07",
    august: "08",
    augustil: "08",
    september: "09",
    septembril: "09",
    oktoober: "10",
    oktoobril: "10",
    november: "11",
    novembril: "11",
    detsember: "12",
    detsembril: "12",
  };

  const textMatch = raw.match(/(\d{1,2})\.\s*(\w+)\s+(\d{4})/);
  if (textMatch) {
    const [, day, monthName, year] = textMatch;
    const monthNum = estonianMonths[monthName!.toLowerCase()];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // Already ISO: yyyy-MM-dd
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return isoMatch[0];
  }

  return null;
}

// ---------------------------------------------------------------------------
// Listing page parsing — competition decisions
// ---------------------------------------------------------------------------

/**
 * Parse the competition cases table from the juhtumid page.
 *
 * The page renders a single HTML <table> with columns:
 *   Kuupäev | Nr | Pealkiri (linked) | Tegevusala
 *
 * Links point to PDFs at /media/{ID}/download or /sites/default/files/...
 */
function parseDecisionListingPage(html: string): DecisionRow[] {
  const $ = cheerio.load(html);
  const rows: DecisionRow[] = [];

  $("table tbody tr").each((_i, tr) => {
    const tds = $(tr).find("td");
    if (tds.length < 4) return;

    const dateRaw = $(tds[0]).text().trim();
    const caseNumber = $(tds[1]).text().trim();
    const titleLink = $(tds[2]).find("a").first();
    const titleText = titleLink.text().trim()
      // Remove file size/type suffix: "Title | 633.21 KB | pdf"
      .replace(/\s*\|\s*[\d.,]+\s*KB\s*\|\s*pdf\s*$/i, "")
      .trim();
    const href = titleLink.attr("href") ?? "";
    const sector = $(tds[3]).text().trim();

    if (!caseNumber && !titleText) return;

    rows.push({
      date: dateRaw,
      case_number: caseNumber || `KA-WEB/${_i}`,
      title: titleText || caseNumber,
      href: href.startsWith("http") ? href : `${BASE_URL}${href}`,
      sector,
    });
  });

  return rows;
}

// ---------------------------------------------------------------------------
// Listing page parsing — mergers
// ---------------------------------------------------------------------------

/**
 * Parse the merger notifications and decisions table.
 *
 * The table has columns:
 *   Teate esitamise kuupäev | Koondumise osalised ja lühikokkuvõte |
 *   AT-s avaldamise alustamise kuupäev | Otsus
 *
 * The parties column and the decision column contain linked PDFs.
 * The AT publication column may contain the AT reference number
 * (e.g. AT2574497) inside the link text.
 */
function parseMergerListingPage(html: string): MergerRow[] {
  const $ = cheerio.load(html);
  const rows: MergerRow[] = [];

  $("table tbody tr").each((_i, tr) => {
    const tds = $(tr).find("td");
    // Table has 5 columns; some rows may have fewer cells (header rows, etc.)
    if (tds.length < 3) return;

    const dateRaw = $(tds[0]).text().trim();
    // Skip rows that don't start with a date
    if (!dateRaw.match(/\d{1,2}\.\d{1,2}\.\d{4}/)) return;

    // Column 2: Parties and summary (linked PDF)
    const partiesLink = $(tds[1]).find("a").first();
    const partiesText = partiesLink.length > 0
      ? partiesLink.text().trim()
          .replace(/\s*\|\s*[\d.,]+\s*KB\s*\|\s*pdf\s*$/i, "")
          .trim()
      : $(tds[1]).text().trim();
    const partiesHref = partiesLink.attr("href") ?? null;

    // Column 3: AT publication start date — extract AT reference number
    const atStartText = $(tds[2]).text().trim();
    const atMatch = atStartText.match(/\(AT\d+\)/);
    const atRef = atMatch ? atMatch[0].replace(/[()]/g, "") : null;

    // Column 5 (index 4): Decision — contains case number and linked PDF
    // Column 4 (index 3) is AT publication end date, which we skip
    let decisionHref: string | null = null;
    let decisionText: string | null = null;
    const decisionIdx = tds.length >= 5 ? 4 : 3;
    if (tds.length >= 4) {
      const decisionLink = $(tds[decisionIdx]).find("a").first();
      if (decisionLink.length > 0) {
        decisionText = decisionLink.text().trim()
          .replace(/\s*\|\s*[\d.,]+\s*KB\s*\|\s*pdf\s*$/i, "")
          .trim();
        decisionHref = decisionLink.attr("href") ?? null;
      } else {
        // Some entries have plain text without a link
        const plainText = $(tds[decisionIdx]).text().trim();
        if (plainText.match(/otsus|5-5/i)) {
          decisionText = plainText;
        }
      }
    }

    if (!partiesText && !decisionText) return;

    rows.push({
      date: dateRaw,
      parties_text: partiesText,
      parties_href: partiesHref
        ? (partiesHref.startsWith("http") ? partiesHref : `${BASE_URL}${partiesHref}`)
        : null,
      at_ref: atRef,
      decision_href: decisionHref
        ? (decisionHref.startsWith("http") ? decisionHref : `${BASE_URL}${decisionHref}`)
        : null,
      decision_text: decisionText || null,
    });
  });

  return rows;
}

// ---------------------------------------------------------------------------
// Decision classification (Estonian competition law context)
// ---------------------------------------------------------------------------

/**
 * Classify the type of a competition decision based on its title and body text.
 *
 * Estonian competition law decision types:
 *   - turgu valitsev seisund / kuritarvitamine  -> abuse_of_dominance
 *   - kokkulepe / kartell / kooskõlastatud tegevus -> cartel
 *   - sektoriuuring / turu-uuring -> sector_inquiry
 *   - väärteomenetlus / trahv -> sanction
 *   - ettekirjutus -> order (administrative order)
 *   - kohustuse siduv muutmine -> commitment_decision
 *   - soovitus -> recommendation
 */
function classifyDecisionType(
  title: string,
  bodyText: string,
): { type: string | null; outcome: string | null } {
  const all = `${title} ${bodyText.slice(0, 3000)}`.toLowerCase();

  // --- Type classification ---
  let type: string | null = null;

  if (
    all.includes("turgu valitsev") ||
    all.includes("kuritarvitami") ||
    all.includes("turgu valitseva seisundi") ||
    all.includes("domineeriv seisund")
  ) {
    type = "abuse_of_dominance";
  } else if (
    all.includes("kartell") ||
    all.includes("kokkulepe") ||
    all.includes("kooskõlastatud tegevus") ||
    all.includes("konkurentsi kahjustav") ||
    all.includes("keelatud kokkulepe")
  ) {
    type = "cartel";
  } else if (
    all.includes("sektoriuuring") ||
    all.includes("turu-uuring") ||
    all.includes("turuanalüüs")
  ) {
    type = "sector_inquiry";
  } else if (
    all.includes("väärteomenetlus") ||
    all.includes("väärtegu") ||
    all.includes("trahv")
  ) {
    type = "sanction";
  } else if (
    all.includes("ettekirjutus")
  ) {
    type = "order";
  } else if (
    all.includes("kohustus") &&
    (all.includes("siduv") || all.includes("siduvaks"))
  ) {
    type = "commitment_decision";
  } else if (
    all.includes("soovitus")
  ) {
    type = "recommendation";
  } else {
    type = "decision";
  }

  // --- Outcome classification ---
  let outcome: string | null = null;

  if (
    all.includes("trahv") ||
    all.includes("rahatrahv") ||
    all.includes("sunniraha")
  ) {
    outcome = "fine";
  } else if (
    all.includes("ettekirjutus") &&
    (all.includes("lõpetamiseks") || all.includes("keelamiseks"))
  ) {
    outcome = "prohibited";
  } else if (
    all.includes("kohustus") &&
    (all.includes("siduv") || all.includes("siduvaks"))
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("menetluse lõpetami") ||
    all.includes("järelevalvemenetluse lõpetami") ||
    all.includes("lõpetamise teade") ||
    all.includes("teade menetluse lõpetamisest")
  ) {
    outcome = "closed";
  } else if (
    all.includes("rikkumist ei tuvastatud") ||
    all.includes("ei tuvastanud rikkumist")
  ) {
    outcome = "cleared";
  } else if (
    all.includes("alustamata jätmi")
  ) {
    outcome = "dismissed";
  }

  return { type, outcome };
}

/**
 * Classify a merger outcome from the decision text.
 *
 * Konkurentsiamet merger outcomes:
 *   - Koondumise keelamine          -> blocked
 *   - Lubatud tingimusega/kohustusega -> cleared_with_conditions
 *   - Koondumise lubamine           -> cleared_phase1
 *   - II faas                       -> cleared_phase2
 */
function classifyMergerOutcome(
  title: string,
  bodyText: string,
): string | null {
  const all = `${title} ${bodyText}`.toLowerCase();

  if (all.includes("keelamis") || all.includes("keelatud") || all.includes("keeld")) {
    return "blocked";
  }
  if (
    all.includes("tingimusega") ||
    all.includes("kohustusega") ||
    all.includes("tingimustega") ||
    all.includes("kohustuste") ||
    all.includes("kohustus")
  ) {
    return "cleared_with_conditions";
  }
  if (all.includes("tagasi võ") || all.includes("tagasivõ") || all.includes("tagasi võetud")) {
    return "withdrawn";
  }
  if (
    all.includes("ii faas") ||
    all.includes("teine faas") ||
    all.includes("täiendav menetlus") ||
    all.includes("süvauuring")
  ) {
    return "cleared_phase2";
  }
  if (
    all.includes("lubatud") ||
    all.includes("luba") ||
    all.includes("heaks kiidetud") ||
    all.includes("lubamine") ||
    all.includes("ei takista")
  ) {
    return "cleared_phase1";
  }

  // Default: most mergers are approved at phase 1
  return "cleared_phase1";
}

// ---------------------------------------------------------------------------
// Sector classification (Estonian keywords)
// ---------------------------------------------------------------------------

/** Map Estonian keywords in title/body/raw-sector to normalised sector IDs. */
function classifySector(
  title: string,
  bodyText: string,
  rawSector?: string,
): string | null {
  const text = `${rawSector ?? ""} ${title} ${bodyText.slice(0, 2000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energy",
      patterns: [
        "energeetika", "energia", "elektri", "gaas", "maagaas",
        "kaugküte", "soojus", "põlevkivi", "tuuleenergia", "taastuvenergia",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telekommunikat", "sideteenus", "mobiil", "lairiba",
        "elektrooniline side", "telia", "elisa", "tele2",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transport", "raudtee", "lennujaam", "bussitransport",
        "parvlaev", "meretransport", "taksondus", "sõidujagami",
        "reisjatevedu", "veondus",
      ],
    },
    {
      id: "financial_services",
      patterns: [
        "pangandus", "pangateenused", "kindlustus", "finantsteenused",
        "maksetariif", "kaardimaks", "liikluskindlustus",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "jäätme", "jäätmekäitl", "jäätmeveo", "prügi",
        "taaskasutami", "pakendi",
      ],
    },
    {
      id: "postal_services",
      patterns: [
        "postside", "post", "kojukanne", "pakiautomaad",
        "pakiedasta", "eesti post",
      ],
    },
    {
      id: "water_utilities",
      patterns: [
        "veemajandus", "ühisveevärk", "vesi", "kanalisatsioon",
      ],
    },
    {
      id: "retail",
      patterns: [
        "jaekaubandus", "kaubandus", "toidukaup", "kauplusekett",
        "e-kaubandus", "hulgimüük",
      ],
    },
    {
      id: "media",
      patterns: [
        "meedia", "reklaam", "raamat", "kirjastami",
        "filmide levitami", "kinoteenu", "kinod",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "tervishoi", "haigla", "ravim", "apteek",
        "veterinaar",
      ],
    },
    {
      id: "port_services",
      patterns: [
        "sadamateenused", "sadam", "pukseeri",
      ],
    },
    {
      id: "digital_economy",
      patterns: [
        "portaal", "e-keskkond", "internetikaubandus",
        "automüügiportaal", "piletimüügikeskkond",
      ],
    },
    {
      id: "food_industry",
      patterns: [
        "toiduaine", "piim", "õlle", "jook",
      ],
    },
    {
      id: "real_estate",
      patterns: [
        "kinnisvara",
      ],
    },
    {
      id: "funeral_services",
      patterns: [
        "matuseteenu", "krematoorium", "morgiteenu", "lahkunu",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const pattern of patterns) {
      if (text.includes(pattern)) return id;
    }
  }

  return rawSector ? rawSector.replace(/\s+/g, "_").slice(0, 50) : null;
}

// ---------------------------------------------------------------------------
// Fine amount extraction (Estonian)
// ---------------------------------------------------------------------------

/**
 * Extract a fine/penalty amount from Estonian text.
 *
 * Handles Estonian number formatting and magnitude words.
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    // "N miljonit eurot" / "N milj eurot"
    /([\d,.\s]+)\s*milj(?:onit|\.)\s*euro/gi,
    // "N miljardit eurot"
    /([\d,.\s]+)\s*miljardit\s*euro/gi,
    // "trahv N eurot"
    /trahv[a-z]*\s+(?:summas\s+)?(?:kuni\s+)?([\d\s.]+(?:,\d+)?)\s*euro/gi,
    // "sunniraha N eurot"
    /sunniraha[a-z]*\s+([\d\s.]+(?:,\d+)?)\s*euro/gi,
    // "N eurot"
    /([\d\s.]+(?:,\d+)?)\s*euro[t]?\b/gi,
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1]) {
      let numStr = match[1].trim();

      if (pattern.source.includes("miljardit")) {
        numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000_000;
      }

      if (pattern.source.includes("milj")) {
        numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000;
      }

      // Direct amount: Estonian uses space/dot as thousands separator,
      // comma for decimal
      numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 1000) return val;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Legal article extraction (Estonian competition law)
// ---------------------------------------------------------------------------

/**
 * Extract cited Estonian competition law (Konkurentsiseadus) articles and
 * EU treaty articles from the decision text.
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  let m: RegExpExecArray | null;

  // Konkurentsiseadus (KonkS) sections: "Konkurentsiseaduse § 16" / "KonkS § 4"
  const konkPattern =
    /(?:konkurentsiseaduse?|KonkS)\s*§\s*(\d+)/gi;
  while ((m = konkPattern.exec(text)) !== null) {
    articles.add(`KonkS § ${m[1]}`);
  }

  // Standalone "§ N" near competition law context
  const sectionPattern = /§\s*(\d+)/g;
  while ((m = sectionPattern.exec(text)) !== null) {
    const num = parseInt(m[1]!, 10);
    // Common Konkurentsiseadus sections: 4 (agreements), 16 (dominance), 17, 18, 21-22 (merger)
    if ([4, 5, 6, 10, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 735, 736, 737].includes(num)) {
      articles.add(`KonkS § ${num}`);
    }
  }

  // EU treaty articles: ELTL artiklid 101/102 (TFEU in Estonian)
  const euPattern =
    /(?:ELTL|EL toimimise lepingu|Euroopa Liidu toimimise lepingu)\s*(?:artikkel|artikli|art\.?)\s*(\d{2,3})/gi;
  while ((m = euPattern.exec(text)) !== null) {
    const artNum = parseInt(m[1]!, 10);
    if (artNum === 101 || artNum === 102) {
      articles.add(`ELTL art ${artNum}`);
    }
  }

  // "Art. 101" / "Art. 102" standalone patterns
  const artPattern = /Art(?:ikkel|ikli)?\.?\s*(101|102)/gi;
  while ((m = artPattern.exec(text)) !== null) {
    articles.add(`ELTL art ${m[1]}`);
  }

  return [...articles];
}

// ---------------------------------------------------------------------------
// Merger party extraction
// ---------------------------------------------------------------------------

/**
 * Extract acquiring party and target from a merger parties text.
 *
 * Konkurentsiamet merger entries use the format "Acquiring / Target" or
 * "Acquiring Party / Target Party (description)".
 */
function extractMergerParties(
  partiesText: string,
): { acquiring: string | null; target: string | null } {
  const slashParts = partiesText.split(/\s*\/\s*/);
  if (slashParts.length >= 2) {
    return {
      acquiring: slashParts[0]!.trim().slice(0, 300),
      target: slashParts
        .slice(1)
        .join(" / ")
        .trim()
        .slice(0, 300),
    };
  }

  return { acquiring: partiesText.trim().slice(0, 300) || null, target: null };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Sector metadata (Estonian / English names)
// ---------------------------------------------------------------------------

const SECTOR_META: Record<string, { name: string; name_en: string }> = {
  energy: { name: "Energeetika", name_en: "Energy" },
  telecommunications: { name: "Telekommunikatsioon", name_en: "Telecommunications" },
  transport: { name: "Transport", name_en: "Transport" },
  financial_services: { name: "Finantsteenused", name_en: "Financial Services" },
  waste_management: { name: "Jäätmekäitlus", name_en: "Waste Management" },
  postal_services: { name: "Postiteenused", name_en: "Postal Services" },
  water_utilities: { name: "Veemajandus", name_en: "Water Utilities" },
  retail: { name: "Jaekaubandus", name_en: "Retail" },
  media: { name: "Meedia", name_en: "Media" },
  healthcare: { name: "Tervishoiuteenused", name_en: "Healthcare" },
  port_services: { name: "Sadamateenused", name_en: "Port Services" },
  digital_economy: { name: "Digitaalmajandus", name_en: "Digital Economy" },
  food_industry: { name: "Toiduainetööstus", name_en: "Food Industry" },
  real_estate: { name: "Kinnisvara", name_en: "Real Estate" },
  funeral_services: { name: "Matuseteenused", name_en: "Funeral Services" },
};

// ---------------------------------------------------------------------------
// Process a single competition decision row
// ---------------------------------------------------------------------------

async function processDecisionRow(
  row: DecisionRow,
): Promise<ParsedDecision | null> {
  const date = parseEstonianDate(row.date);
  const sectorId = classifySector(row.title, "", row.sector);

  // Attempt to fetch and parse the linked PDF for full text
  let fullText = row.title;
  let summary: string | null = null;

  if (row.href) {
    const pdfText = await fetchPdfText(row.href);
    if (pdfText && pdfText.length > 50) {
      fullText = pdfText;
      summary = pdfText.slice(0, 500).replace(/\s+/g, " ").trim();
    }
  }

  // If PDF extraction failed, use the title as both title and full_text
  if (fullText.length < 30) {
    fullText = `${row.title}. Konkurentsiamet, ${row.case_number}, ${row.date}. Tegevusala: ${row.sector}.`;
  }

  // Extract parties from the title (many Estonian decisions name parties in the title)
  const partiesFromTitle = extractPartiesFromDecisionTitle(row.title);

  const { type, outcome } = classifyDecisionType(row.title, fullText);
  const fineAmount = extractFineAmount(fullText);
  const legalArticles = extractLegalArticles(fullText);

  return {
    case_number: row.case_number,
    title: row.title,
    date,
    type,
    sector: sectorId,
    parties: partiesFromTitle
      ? JSON.stringify(partiesFromTitle)
      : null,
    summary,
    full_text: fullText,
    outcome: outcome ?? (fineAmount ? "fine" : null),
    fine_amount: fineAmount,
    gwb_articles:
      legalArticles.length > 0
        ? JSON.stringify(legalArticles)
        : null,
    status: "final",
  };
}

/**
 * Extract party names from a decision title.
 *
 * Konkurentsiamet decision titles often name the involved parties:
 *   - "Ettekirjutus Eesti Keskkonnateenused AS-ile..."
 *   - "Teade menetluse lõpetamisest Telia Eesti AS"
 *   - "OÜ Eurex CS / AS G4S Eesti"
 */
function extractPartiesFromDecisionTitle(title: string): string[] | null {
  // Pattern: "X / Y" (explicit party separator)
  if (title.includes(" / ")) {
    const parts = title.split(" / ").map((p) => p.trim()).filter(Boolean);
    if (parts.length >= 2) return parts.map((p) => p.slice(0, 200));
  }

  // Pattern: Find Estonian company suffixes (AS, OÜ, MTÜ, SA) followed by a name
  const companyPattern = /(?:AS|OÜ|MTÜ|SA|A\/S)\s+[A-ZÄÖÜÕ][^\s,|]{2,}(?:\s+[A-ZÄÖÜÕ][^\s,|]{2,})*/g;
  const matches = title.match(companyPattern);
  if (matches && matches.length > 0) {
    return [...new Set(matches.map((m) => m.trim()))].slice(0, 5);
  }

  // Pattern: Name before suffix: "Eesti Energia AS"
  const companySuffixPattern = /[A-ZÄÖÜÕ][^\s,|]{2,}(?:\s+[A-ZÄÖÜÕ][^\s,|]{1,})*\s+(?:AS|OÜ|MTÜ|SA)/g;
  const suffixMatches = title.match(companySuffixPattern);
  if (suffixMatches && suffixMatches.length > 0) {
    return [...new Set(suffixMatches.map((m) => m.trim()))].slice(0, 5);
  }

  return null;
}

// ---------------------------------------------------------------------------
// Process a single merger row
// ---------------------------------------------------------------------------

async function processMergerRow(
  row: MergerRow,
  index: number,
): Promise<ParsedMerger | null> {
  const date = parseEstonianDate(row.date);
  const { acquiring, target } = extractMergerParties(row.parties_text);

  // Build a title from the parties
  const title = row.parties_text || `Koondumine ${row.at_ref ?? `#${index}`}`;

  // Generate a case number from the AT reference or decision text
  let caseNumber: string;
  if (row.decision_text) {
    // Try to extract "otsus nr 5-5/2026-010" from the decision text
    const otsusMatch = row.decision_text.match(/(?:otsus\s+nr\s+|nr\s+)([\d\-\/]+)/i);
    if (otsusMatch) {
      caseNumber = otsusMatch[1]!;
    } else if (row.at_ref) {
      caseNumber = `KO-${row.at_ref}`;
    } else {
      caseNumber = `KO-${date ?? "unknown"}-${index}`;
    }
  } else if (row.at_ref) {
    caseNumber = `KO-${row.at_ref}`;
  } else {
    caseNumber = `KO-${date ?? "unknown"}-${index}`;
  }

  // Determine sector from the parties description
  const sectorId = classifySector(title, "", undefined);

  // Try to fetch PDF content for the decision
  let fullText = title;
  let summary: string | null = null;

  // Prefer the decision PDF, fall back to the parties notification PDF
  const pdfUrl = row.decision_href ?? row.parties_href;
  if (pdfUrl) {
    const pdfText = await fetchPdfText(pdfUrl);
    if (pdfText && pdfText.length > 50) {
      fullText = pdfText;
      summary = pdfText.slice(0, 500).replace(/\s+/g, " ").trim();
    }
  }

  if (fullText.length < 30) {
    fullText = `Koondumise teade: ${title}. Kuupäev: ${row.date}. ${row.at_ref ? `Viide: ${row.at_ref}.` : ""}`;
  }

  const outcome = classifyMergerOutcome(title, fullText);

  return {
    case_number: caseNumber,
    title,
    date,
    sector: sectorId,
    acquiring_party: acquiring,
    target,
    summary,
    full_text: fullText,
    outcome,
    turnover: null, // Turnover not reliably extractable from HTML
  };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== Konkurentsiamet (ECA) Competition Decisions Crawler ===");
  console.log(`  Database:    ${DB_PATH}`);
  console.log(`  Dry run:     ${dryRun}`);
  console.log(`  Resume:      ${resume}`);
  console.log(`  Force:       ${force}`);
  console.log(
    `  Max pages:   ${maxPagesOverride ?? "all (single-page tables)"}`,
  );
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedCaseNumbers);

  // -----------------------------------------------------------------------
  // Step 1: Fetch and parse listing pages
  // -----------------------------------------------------------------------

  console.log("Step 1: Fetching listing pages...\n");

  // 1a: Competition decisions
  console.log(`  Fetching competition decisions from: ${LISTING_PAGES.decisions.url}`);
  const decisionsHtml = await rateLimitedFetch(LISTING_PAGES.decisions.url);
  let decisionRows: DecisionRow[] = [];
  if (decisionsHtml) {
    decisionRows = parseDecisionListingPage(decisionsHtml);
    console.log(`  Parsed ${decisionRows.length} competition decision entries`);
  } else {
    console.warn("  [WARN] Could not fetch competition decisions page");
  }

  // 1b: Merger decisions
  console.log(`  Fetching merger decisions from: ${LISTING_PAGES.mergers.url}`);
  const mergersHtml = await rateLimitedFetch(LISTING_PAGES.mergers.url);
  let mergerRows: MergerRow[] = [];
  if (mergersHtml) {
    mergerRows = parseMergerListingPage(mergersHtml);
    console.log(`  Parsed ${mergerRows.length} merger entries`);
  } else {
    console.warn("  [WARN] Could not fetch merger decisions page");
  }

  // Apply --max-pages as a row limit (since these are single-page tables)
  if (maxPagesOverride) {
    decisionRows = decisionRows.slice(0, maxPagesOverride * 10);
    mergerRows = mergerRows.slice(0, maxPagesOverride * 10);
    console.log(`  Limited to ${decisionRows.length} decisions and ${mergerRows.length} mergers (--max-pages ${maxPagesOverride})`);
  }

  const totalItems = decisionRows.length + mergerRows.length;
  console.log(`\n  Total items to process: ${totalItems}`);

  if (totalItems === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // -----------------------------------------------------------------------
  // Step 2: Initialize database (unless dry run)
  // -----------------------------------------------------------------------

  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // -----------------------------------------------------------------------
  // Step 3: Process competition decisions
  // -----------------------------------------------------------------------

  let decisionsIngested = 0;
  let mergersIngested = 0;
  let errors = 0;
  let skipped = 0;

  console.log("\nStep 3a: Processing competition decisions...\n");

  for (let i = 0; i < decisionRows.length; i++) {
    const row = decisionRows[i]!;
    const progress = `[${i + 1}/${decisionRows.length}]`;

    // Skip already-processed (resume)
    if (resume && processedSet.has(row.case_number)) {
      skipped++;
      continue;
    }

    console.log(`${progress} ${row.case_number} | ${row.title.slice(0, 80)}`);

    try {
      const decision = await processDecisionRow(row);

      if (!decision) {
        console.log("  SKIP -- could not parse");
        skipped++;
        continue;
      }

      if (dryRun) {
        console.log(
          `  DECISION: ${decision.case_number} | type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}`,
        );
      } else {
        const stmt = force
          ? stmts!.upsertDecision
          : stmts!.insertDecision;
        stmt.run(
          decision.case_number,
          decision.title,
          decision.date,
          decision.type,
          decision.sector,
          decision.parties,
          decision.summary,
          decision.full_text,
          decision.outcome,
          decision.fine_amount,
          decision.gwb_articles,
          decision.status,
        );
        console.log(`  INSERTED decision: ${decision.case_number}`);
      }

      decisionsIngested++;
      processedSet.add(row.case_number);
      state.processedCaseNumbers.push(row.case_number);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`decision_error: ${row.case_number}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 items)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested += decisionsIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} decisions`);
      decisionsIngested = 0; // Reset to avoid double-counting
    }
  }

  // -----------------------------------------------------------------------
  // Step 3b: Process merger decisions
  // -----------------------------------------------------------------------

  console.log("\nStep 3b: Processing merger decisions...\n");

  for (let i = 0; i < mergerRows.length; i++) {
    const row = mergerRows[i]!;
    const progress = `[${i + 1}/${mergerRows.length}]`;

    // Build a preliminary case key for resume checks
    const prelimKey = row.at_ref ?? `merger-${i}`;
    if (resume && processedSet.has(prelimKey)) {
      skipped++;
      continue;
    }

    console.log(`${progress} ${row.at_ref ?? "no-ref"} | ${row.parties_text.slice(0, 80)}`);

    try {
      const merger = await processMergerRow(row, i);

      if (!merger) {
        console.log("  SKIP -- could not parse");
        skipped++;
        continue;
      }

      // Use final case number for resume tracking
      const trackingKey = merger.case_number;

      if (resume && processedSet.has(trackingKey)) {
        skipped++;
        continue;
      }

      if (dryRun) {
        console.log(
          `  MERGER: ${merger.case_number} | acquiring=${merger.acquiring_party?.slice(0, 50)}, outcome=${merger.outcome}`,
        );
      } else {
        const stmt = force
          ? stmts!.upsertMerger
          : stmts!.insertMerger;
        stmt.run(
          merger.case_number,
          merger.title,
          merger.date,
          merger.sector,
          merger.acquiring_party,
          merger.target,
          merger.summary,
          merger.full_text,
          merger.outcome,
          merger.turnover,
        );
        console.log(`  INSERTED merger: ${merger.case_number}`);
      }

      mergersIngested++;
      processedSet.add(trackingKey);
      state.processedCaseNumbers.push(trackingKey);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`merger_error: ${prelimKey}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 items)
    if ((i + 1) % 25 === 0) {
      state.mergersIngested += mergersIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} mergers`);
      mergersIngested = 0;
    }
  }

  // -----------------------------------------------------------------------
  // Step 4: Update sector counts from the database
  // -----------------------------------------------------------------------

  if (!dryRun && db && stmts) {
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const meta = SECTOR_META[id];
        stmts!.upsertSector.run(
          id,
          meta?.name ?? id,
          meta?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nUpdated ${Object.keys(finalSectorCounts).length} sector records`,
    );
  }

  // -----------------------------------------------------------------------
  // Step 5: Final state save
  // -----------------------------------------------------------------------

  state.decisionsIngested += decisionsIngested;
  state.mergersIngested += mergersIngested;
  saveState(state);

  // -----------------------------------------------------------------------
  // Step 6: Summary
  // -----------------------------------------------------------------------

  if (!dryRun && db) {
    const decisionCount = (
      db
        .prepare("SELECT count(*) as cnt FROM decisions")
        .get() as { cnt: number }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${state.decisionsIngested}`);
    console.log(`  New mergers:      ${state.mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log("\nDone.");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
