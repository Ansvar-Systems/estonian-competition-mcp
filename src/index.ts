#!/usr/bin/env node

/**
 * Estonian Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying ECA (Konkurentsiamet — Estonian Competition
 * Authority) decisions, merger control cases, and sector enforcement activity
 * under Estonian competition law (Konkurentsiseadus).
 *
 * Tool prefix: ee_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchMergers,
  getMerger,
  listSectors,
} from "./db.js";
import { buildCitation } from "./citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "estonian-competition-mcp";

// --- Tool definitions ---------------------------------------------------------

const TOOLS = [
  {
    name: "ee_comp_search_decisions",
    description:
      "Full-text search across ECA (Konkurentsiamet) enforcement decisions covering abuse of dominance, cartel enforcement, and sector inquiries under the Konkurentsiseadus (Competition Act). Returns matching decisions with case number, parties, outcome, fine amount, and legal basis cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'turgu valitsev seisund', 'kartell', 'hinnakokkulepe')",
        },
        type: {
          type: "string",
          enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"],
          description: "Filter by decision type. Optional.",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'energy', 'telecommunications', 'transport'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"],
          description: "Filter by outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "ee_comp_get_decision",
    description:
      "Get a specific ECA decision by case number (e.g., '5-5/2023', '5-1/2022').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "ECA case number (e.g., '5-5/2023', '5-1/2022')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "ee_comp_search_mergers",
    description:
      "Search ECA merger control decisions (koondumise kontroll). Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'koondumine energeetika', 'telekomiturg', 'jaekaubandus')",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'energy', 'telecommunications', 'retail'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"],
          description: "Filter by merger outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "ee_comp_get_merger",
    description:
      "Get a specific ECA merger control decision by case number (e.g., 'KO-1/2023').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "ECA merger case number (e.g., 'KO-1/2023')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "ee_comp_list_sectors",
    description:
      "List all sectors with ECA enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "ee_comp_about",
    description:
      "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation --------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  case_number: z.string().min(1),
});

const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetMergerArgs = z.object({
  case_number: z.string().min(1),
});

// --- Helper ------------------------------------------------------------------

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// --- Server setup ------------------------------------------------------------

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "ee_comp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({
          query: parsed.query,
          type: parsed.type,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "ee_comp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.case_number);
        if (!decision) {
          return errorContent(`Decision not found: ${parsed.case_number}`);
        }
        const dec = decision as Record<string, unknown>;
        return textContent({
          ...dec,
          _citation: buildCitation(
            String(dec.case_number ?? parsed.case_number),
            String(dec.title ?? dec.case_number ?? parsed.case_number),
            "ee_comp_get_decision",
            { case_number: parsed.case_number },
            dec.url != null ? String(dec.url) : undefined,
          ),
        });
      }

      case "ee_comp_search_mergers": {
        const parsed = SearchMergersArgs.parse(args);
        const results = searchMergers({
          query: parsed.query,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "ee_comp_get_merger": {
        const parsed = GetMergerArgs.parse(args);
        const merger = getMerger(parsed.case_number);
        if (!merger) {
          return errorContent(`Merger case not found: ${parsed.case_number}`);
        }
        const m = merger as Record<string, unknown>;
        return textContent({
          ...m,
          _citation: buildCitation(
            String(m.case_number ?? parsed.case_number),
            String(m.title ?? m.case_number ?? parsed.case_number),
            "ee_comp_get_merger",
            { case_number: parsed.case_number },
            m.url != null ? String(m.url) : undefined,
          ),
        });
      }

      case "ee_comp_list_sectors": {
        const sectors = listSectors();
        return textContent({ sectors, count: sectors.length });
      }

      case "ee_comp_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "ECA (Konkurentsiamet — Estonian Competition Authority) MCP server. Provides access to Estonian competition law enforcement decisions, merger control cases, and sector enforcement data under the Konkurentsiseadus (Competition Act).",
          data_source: "Konkurentsiamet (https://www.konkurentsiamet.ee/)",
          coverage: {
            decisions: "Abuse of dominance, cartel enforcement, and sector inquiries under Konkurentsiseadus",
            mergers: "Merger control decisions (koondumise kontroll) — Phase I and Phase II",
            sectors: "Energy, telecommunications, transport, retail, financial services, media",
          },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// --- Main --------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
