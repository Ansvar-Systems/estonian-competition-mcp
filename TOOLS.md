# Tools Reference

This document lists all tools exposed by the Estonian Competition MCP server.

## Tool Prefix: `ee_comp_`

All tools are prefixed with `ee_comp_` (Estonia — competition authority).

---

## ee_comp_search_decisions

Full-text search across ECA (Konkurentsiamet) enforcement decisions covering abuse of dominance, cartel enforcement, and sector inquiries under the Konkurentsiseadus (Competition Act).

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g., `turgu valitsev seisund`, `kartell`) |
| `type` | string | No | Filter by type: `abuse_of_dominance`, `cartel`, `merger`, `sector_inquiry` |
| `sector` | string | No | Filter by sector ID (e.g., `energy`, `telecommunications`) |
| `outcome` | string | No | Filter by outcome: `prohibited`, `cleared`, `cleared_with_conditions`, `fine` |
| `limit` | number | No | Maximum results (default: 20, max: 100) |

**Returns:** `{ results: Decision[], count: number, _meta: ... }`

---

## ee_comp_get_decision

Retrieve a specific ECA enforcement decision by case number.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | Yes | ECA case number (e.g., `5-5/2023`, `5-1/2022`) |

**Returns:** `Decision & { _citation: ..., _meta: ... }`

---

## ee_comp_search_mergers

Search ECA merger control decisions (koondumise kontroll).

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g., `koondumine energeetika`, `telekomiturg`) |
| `sector` | string | No | Filter by sector ID |
| `outcome` | string | No | Filter by outcome: `cleared`, `cleared_phase1`, `cleared_with_conditions`, `prohibited` |
| `limit` | number | No | Maximum results (default: 20, max: 100) |

**Returns:** `{ results: Merger[], count: number, _meta: ... }`

---

## ee_comp_get_merger

Retrieve a specific ECA merger control decision by case number.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | Yes | ECA merger case number (e.g., `KO-1/2023`) |

**Returns:** `Merger & { _citation: ..., _meta: ... }`

---

## ee_comp_list_sectors

List all sectors with ECA enforcement activity.

**Parameters:** None

**Returns:** `{ sectors: Sector[], count: number, _meta: ... }`

---

## ee_comp_about

Return metadata about this MCP server: version, data source, coverage summary, and tool list.

**Parameters:** None

**Returns:** `{ name, version, description, data_source, coverage, tools, _meta }`

---

## ee_comp_list_sources

List the official data sources used by this MCP server.

**Parameters:** None

**Returns:** `{ sources: Source[], _meta: ... }`

---

## ee_comp_check_data_freshness

Check when the database was last updated and ingestion counts.

**Parameters:** None

**Returns:** `{ last_ingest, decisions_count, mergers_count, status, _meta }`
