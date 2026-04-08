# Data Coverage

This document describes the data coverage of the Estonian Competition MCP server.

## Authority

**Konkurentsiamet** (Estonian Competition Authority — ECA)  
Official website: https://www.konkurentsiamet.ee/  
Country: Estonia (EE)  
Jurisdiction: National competition enforcement under Konkurentsiseadus (Competition Act)

---

## Enforcement Decisions

| Metric | Value |
|--------|-------|
| Total decisions | ~197 (as of 2026-03-23) |
| Decision types | Abuse of dominance, cartel enforcement, sector inquiries |
| Legal basis | Konkurentsiseadus (Competition Act) |
| Language | Estonian (et) |
| Date range | Historical to present |

### Decision Types Covered

- **Abuse of dominance** (`abuse_of_dominance`) — Violations of §16–18 Konkurentsiseadus
- **Cartel enforcement** (`cartel`) — Horizontal and vertical agreements
- **Sector inquiries** (`sector_inquiry`) — Market investigations

---

## Merger Control

| Metric | Value |
|--------|-------|
| Total merger decisions | In progress (ingestion pending) |
| Coverage | Phase I and Phase II decisions (koondumise kontroll) |
| Legal basis | Konkurentsiseadus Chapter 4 |

---

## Sectors

Sectors with known ECA enforcement activity:

- Energy (`energy`)
- Telecommunications (`telecommunications`)
- Transport (`transport`)
- Retail (`retail`)
- Financial services (`financial_services`)
- Media (`media`)

---

## Data Freshness

- Database updates are periodic via the ingestion pipeline (`npm run check-updates`)
- Last known ingest: 2026-03-23
- Use `ee_comp_check_data_freshness` tool to query current state at runtime

---

## Limitations

- Coverage may be incomplete for older decisions not published digitally
- Merger decisions ingestion is in progress — counts may be zero
- Data sourced from official publications; processing may introduce minor errors
- Not a substitute for consulting official Konkurentsiamet sources directly
