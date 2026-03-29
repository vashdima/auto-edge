# Entry Management Automation — High-Level Plan

Automate data collection and review using Oanda candles + port of `entry_mgmt.pine`, with a React frontend for discretionary review, CSV export, and separate entry/exit rule optimisers.

---

## 1. Full workflow (overview)

```
[ Phase A: Oanda download ]
      ↓
Aligned candle data (per symbol / chart bar)
      ↓
[ SQL: aligned_candles ]  ← A2 writes; B, C, frontend, optimisers read
      ↓
[ Phase B: Entry logic — scanner ]
      ↓
[ SQL: scan_runs + raw_trades ]  ← B writes with run_id; prints run_id at end
      ↓
[ Phase C: Entry maps ]  ← reads raw_trades (by run_id) + aligned_candles from SQL; marries → journal
      ↓
JSON entry maps (context, validation, status, OHLC buffer, trade result)
      ↓
[ Phase D: Frontend — table + charts (Lightweight Charts) ]
      ↓
Run selector in UI: user sets run_id to change which scan’s entries are shown
      ↓
Review + discretionary filter → Pick trades P
      ↓
Export P to CSV
      ↓
      ├──→ [ Phase E: Exit optimiser ]  →  Best SL/BE/TP rules
      │
      └──→ [ Phase F: Entry optimiser ] →  Best entry rules (match picks P)
```

#### Running the pipeline (script order)

Run from `pine/entry-mgmt-automation`. Execute in this order:

| Order | Phase / Stage | Script | Notes |
|-------|----------------|--------|--------|
| 1 | A2 | `python3 mtf_loader.py` | Fetches Oanda, aligns TFs, writes `aligned_candles`. Also prints B1.0-style load check. |
| 2 | B1.1 | `python3 scanner_indicators.py` | Loads from DB, computes chart/context/validation indicators, writes back to `aligned_candles`. |
| 3 | B1.2–B1.5 | `python3 scanner_entry_logic.py` | B1-only: loads from DB, adds chart/context/validation/entry_setup_detected, prints counts and samples. |
| 4 | B (full) | `python3 scanner_entry_mgmt.py` | Single Phase B entry: runs B1.2–B1.5 then B2 (when implemented). Use this for full scan + persist. |
| 5 | C1 | `python3 entry_maps.py` | Builds entry map JSON on the fly from `raw_trades` + `aligned_candles`; no separate journal table required. Use `--run-key` or `--run-id` to output JSON. |
| 6 | C2 | `uvicorn api:app --host 0.0.0.0 --port 8000` or `python3 api.py` | Serves GET /runs and GET /entries. Frontend can list runs and fetch entry maps by run_id or run_key. |
| 7 | D1 | `cd frontend && npm run dev` | React app: run selector, Trade History table, chart placeholders. Requires C2 API running (port 8000). |
| — | Docker | `docker compose up --build` (from entry-mgmt-automation) | Runs API (port 8000) and frontend (port 80). Mounts ./data and ./config.yaml. Open http://localhost:80. |
| 8+ | D2, … | See per-phase **Run** below | Table refinements, charts (D3), export, optimisers. |

- **Not run as steps:** `oanda_client.py` is a library used by `mtf_loader.py`; run it only to test the Oanda client in isolation.
- **Optional later:** A single runner (e.g. `run_pipeline.py`) could invoke A2 → B1.1 → … in one command.

---

## 2. Implementation steps

### Phase A: Oanda download

**Input:** Config (timeframes, scan range, MA periods for buffer) + `.env` (API token, account, environment). **Output:** Aligned candle data per symbol and timeframe (chart-TF, context-TF, validation-TF) for the fetch range; scan window [from, to] is defined by config.

#### Config (before implementation)

- **Config file** (e.g. `config.yaml` or `config.json`) should define:
  - **Timeframes:** `entry` (chart TF, e.g. M15, H1), `context` (HTF, e.g. W), `validation` (e.g. D). These drive which Oanda granularities to request.
  - **Scan date range:** `from` and `to` (e.g. ISO 8601 UTC or date-only). Entry logic (Phase B) runs only on bars inside this window. The **fetch** range is extended backwards by a buffer so MAs are valid at the start of the scan (see below).
  - **MA / indicator settings:** All periods and multipliers from Pine should be in config. Use **named periods** so DB column names stay fixed when you change period values: `slowEMAPeriod`, `mediumEMAPeriod`, `fastEMAPeriod` (e.g. 20, 50, 100), plus `atrPeriod`, `atrMultiplier`, `maxCandlesAfterBreak`, `maxDistanceFromEMA`, `rrBreakEven`, `rrTakeProfit`. Context and validation use slow/fast EMAs on their TF. The **largest** of the three EMA periods drives the fetch buffer size.
- **Fetch buffer for scan start:** The largest MA you need (e.g. EMA 100 on chart TF, EMA 100 on context TF, EMA 20 on validation TF) requires that many bars of history **before** the first bar you scan. So for each timeframe, request candles from **(from − buffer)** to **to**, where buffer = enough bars for the largest MA on that TF (e.g. 100 for chart and context if using EMA100, 20 for validation; add a small safety margin if desired). **Scan** (run entry logic and state machine) only on bars whose time is in [from, to]; the extra bars before `from` are used only for indicator warm-up.
- All scanner behaviour (symbols list, TFs, scan window, MA/indicator settings) should be read from config so runs are reproducible and optimisers can override them.
- **Template:** See [config.template.yaml](config.template.yaml) for all params that map to `entry_mgmt.pine` inputs (entry detection, risk management, timeframes, scan range).
- **Secrets:** Copy [.env.example](.env.example) to `.env` in this folder and set Oanda API token, account ID, and environment. `.env` is in the repo `.gitignore` and must not be committed.

**Run:** `python3 mtf_loader.py` (implements A2; A1 is the library [oanda_client.py](oanda_client.py) used by mtf_loader).

#### Steps

| Step | Description |
|------|-------------|
| A1 | **Oanda client** — Auth, fetch candles for instrument + granularity + date range. Return normalized OHLC (e.g. pandas DataFrame or list of bars). Map granularity (M15, H1, D, W) to Oanda enum. Support `from` / `to` from config; handle pagination when range exceeds API limit (see below). Auth from `.env`. |
| A2 | **Multi-timeframe loader** — For each symbol + chart TF: fetch chart-TF, context-TF, validation-TF candles for the **fetch range** (config `from` minus buffer to config `to`). Align so for each chart bar you have current HTF and validation bar (no lookahead). **Output:** Aligned candle data ready for Phase B. Persist to SQLite table `aligned_candles` (see below). |

#### Most efficient way to fetch Oanda data

- **One request per (instrument, granularity, range):** Use `GET v3/instruments/{instrument}/candles` with `from`, `to`, `granularity`. Do not send `count` when using `from`+`to`. Max per response often 5000.
- **Fetch range = config range + buffer.** Optionally cache by (instrument, granularity, from, to). Paginate when > 5000 bars; Keep-Alive, ~100 req/s.

#### Where to store candle data after Phase A (for scanner to use)

**Decision: Store aligned A2 output in SQL (SQLite).**

- **Database:** SQLite, single file. Default path: `data/trendfinder.db` (relative to entry-mgmt-automation); override via config `database.path`. WAL mode enabled for concurrent reads during writes.
- **Table:** `aligned_candles`. One row per chart bar per symbol per **chart timeframe** (so M15 and H1 data are distinct). Columns: `symbol`, `chart_tf`, `time` (ISO8601 UTC), `open`, `high`, `low`, `close`, `volume`, then `ctx_*`, `val_*` (context/validation bar data). **Chart indicators** (computed once, stored so C does not recompute): `ema_slow`, `ema_medium`, `ema_fast`, `atr` — column names are fixed (config uses `slowEMAPeriod`, `mediumEMAPeriod`, `fastEMAPeriod` so changing period values does not require DB schema change). Primary key: `(symbol, chart_tf, time)`. Phase B and C query by `(symbol, chart_tf, time range)`.
- **A2 write:** Writes OHLC + ctx_* + val_* only. **Indicator columns** are filled at the **start of Phase B** (after loading aligned data): compute ema_slow, ema_medium, ema_fast, atr from config; write back to `aligned_candles` (or a joined table) for that scan run. Phase C and "View chart" then read OHLC + indicators from DB with no recomputation.

---

### Phase B: Entry logic (scanner)

**Input:** Aligned candle data from **SQL** (table `aligned_candles`) for scan window + config (MA/indicator and risk settings). **Output:** Raw trade records written to **SQL** (tables `scan_runs` + `raw_trades`) with a **run_id**; Phase B **prints the run_id at end** so the user can pass it to C or select it in the frontend.

**Data source for B1.2+ (scanner):** Load aligned data for the scan window via `load_aligned_for_scan()` (or `load_aligned_from_db()`). The loader returns base OHLC + ctx_* + val_* **and**, when present in the table, indicator columns from the DB: `ema_slow`, `ema_medium`, `ema_fast`, `atr`, `ctx_ema_slow`, `ctx_ema_fast`, `val_ema_slow`. So **B1.2 (B1.2a–B1.2e), B1.3, B1.4, and B1.5 use atr and EMA values from the database**; they do not recompute indicators. Run B1.1 first so the DB has these columns populated. **B1.1** loads the full buffer+scan range via `load_aligned_full_buffer()` so indicator columns are populated for all aligned rows, enabling context/validation charts to show EMAs from the first bar.

**Run (per stage):** B1.0 — no separate script (demo in `mtf_loader.py`). B1.1 — `python3 scanner_indicators.py`. B1.2–B1.5 — `python3 scanner_entry_logic.py` (B1-only: chart/context/validation/entry_setup_detected). Full Phase B — `python3 scanner_entry_mgmt.py` (B1.2–B1.5 then B2 when implemented; calls init_b2_tables before first B2 write).

#### B1: Indicators + entry logic (staged)

B1 is split into stages so each part can be tested and debugged in isolation. Reference: [entry_mgmt.pine](../entry_mgmt.pine).

| Stage | Description | Run | Checkpoint |
|-------|-------------|-----|------------|
| **B1.0** | **Data + wiring** — Load aligned data from SQLite for scan window: `load_aligned_from_db(symbols, chart_tf, from_time, to_time)` (or equivalent), wired to config (scan range, symbols, chart_tf). | Demo in `mtf_loader.py` | Assert row count and time range; optionally compare a few rows to fetch path. |
| **B1.1** | **Chart indicators** — On chart series from aligned data: compute ema_slow, ema_medium, ema_fast and ATR from config (`slowEMAPeriod`, `mediumEMAPeriod`, `fastEMAPeriod`, `atrPeriod`). Output columns: `ema_slow`, `ema_medium`, `ema_fast`, `atr`. Write to DB (aligned_candles or joined table) at start of B so C/frontend read without recomputing. | `python3 scanner_indicators.py` | Unit test: known closes → expected EMAs/ATR; no entry logic yet. |
| **B1.2** | **Chart entry logic** — Breakout above ema_slow (and reset when close &lt; ema_slow); bars_since_breakout, withinBreakoutWindow; pause (high &lt; high_prev); distance filter (close − ema_slow in ATR ≤ max); all EMAs up (ema_slow, ema_medium, ema_fast sloping up); price above ema_slow. **Uses ema_slow, ema_medium, ema_fast, atr from DB** (B1.1). | `python3 scanner_entry_logic.py` | Run on one symbol; assert/print bars where chart-only setup is true; no context/validation. |
| **B1.3** | **Context (HTF)** — Build context bar series (one per ctx_time); compute ctx_ema_slow, ctx_ema_fast on context closes (no lookahead). Per chart bar: context_bullish = ctx_ema_slow &gt; ctx_ema_fast and ctx_close &gt; ctx_ema_slow. **Uses ctx_ema_slow, ctx_ema_fast (and ctx_close) from DB** (B1.1). | Included in `python3 scanner_entry_logic.py` | Unit test: synthetic ctx bars → expected context_bullish; or spot-check vs Pine. |
| **B1.4** | **Validation** — Build validation bar series (one per val_time); compute val_ema_slow; lock/unlock (full candle below/above val_ema_slow); val_ema_slow slope up; price above val_ema_slow. validation_ok = not locked and slope up and price above. **Uses val_ema_slow, val_open, val_high, val_low, val_close from DB** (B1.1 / aligned data). | Included in `python3 scanner_entry_logic.py` | Unit test: synthetic val bars; lock/unlock, slope, price above, validation_ok. |
| **B1.5** | **Combine** — entry_setup_detected = (chart setup from B1.2) and context_bullish and validation_ok. Expose candidate setup bars for B2. | Included in `python3 scanner_entry_logic.py` | Print entry_setup_detected count and sample bars; compare to Pine on same symbol/range; optional integration test. |

#### B2: State machine + persist

**Run:** `python3 scanner_entry_mgmt.py` (single Phase B entry point; runs B1.2–B1.5 then B2 when implemented). Implement in the following steps:

| Step | Description | Run / Checkpoint |
|------|-------------|------------------|
| **B2.1** | **Schema** — Create tables in same DB (mtf_loader). **scan_runs:** run_id (PK), scan_from, scan_to, created_at — one row per run for run selector (Phase C lists runs without querying raw_trades). **raw_trades:** id (PK AUTOINCREMENT), run_id (for C to filter), symbol, chart_tf, context_tf, validation_tf, setup_time, entry_time, entry_price, sl, tp, sl_size, exit_reason, rr, context_bullish, validation_ok. Only completed trades written (run to completion before insert; no state/IN_TRADE in DB). Init in mtf_loader; scanner_entry_mgmt.py calls it before first B2 write. | Schema init in mtf_loader; scanner_entry_mgmt.py calls it before first B2 write; assert tables exist and have expected columns. |
| **B2.2** | **State machine (in-memory)** — For aligned data with entry_setup_detected: NO_TRADE → PENDING when entry_setup_detected; set entry time/price (e.g. setup bar or next bar), compute sl/tp from config (atr, multiplier). Transition to IN_TRADE. Emit one trade record per setup (in-memory or dict); no DB yet. Dedupe / one-trade-per-setup rules as needed. | candidate_trades_from_df returns list of dicts; scanner_entry_mgmt prints count and sample; unit tests for entry_price/setup_time/sl/tp. |
| **B2.3** | **Run trade to completion** — For each trade in IN_TRADE, simulate bars from aligned data: apply exit rules (config SL, BE, TP) to get exit_reason and rr. Run to completion only; then write to raw_trades (no IN_TRADE rows in DB). | Unit test: known OHLC series → expected exit_reason, rr. |
| **B2.4** | **Persist + script** — Insert one row into `scan_runs`. For each completed trade, insert into `raw_trades`. Print run_id at end. Wire in scanner_entry_mgmt.py: load from DB, run B1.2–B1.5, then B2.2–B2.4. | `python3 scanner_entry_mgmt.py`; assert run_id and row counts. |

---

### Phase C: Entry maps

**Run:** For CLI: `python3 entry_maps.py --run-key <key>` or `--run-id <id>`. For HTTP API (C2): `uvicorn api:app --host 0.0.0.0 --port 8000` (or `python3 api.py`); serves **GET /runs** (list runs for selector) and **GET /entries?run_id=...** or **?run_key=...** (entry maps for that run). Entry maps are **built on the fly** from `raw_trades` + `aligned_candles`; no separate journal table is required. Optionally add an `entries` table later for caching.

**Input:** Run identifier: **run_id** (int) or **run_key** (str). If run_key, resolve via `SELECT run_id FROM scan_runs WHERE run_key = ?`. **Output:** List of entry map dicts (JSON-serialisable) for the frontend.

| Step | Description |
|------|-------------|
| C1 | **Build entry map per trade** — For each raw record (from `raw_trades` for the chosen run_id): identity (symbol, chartTF, contextTF, validationTF, run_id), contextBullish, validationOk, state, setupTime, entryTime, entryDay, entryPrice, sl, tp, slSize, beActive, rr, exitReason. Add **chart buffer**: query `aligned_candles` for symbol and time range (slowest EMA bars before entry → exit_time); rows include OHLC and indicator columns. Context/validation buffers = aggregate aligned rows by ctx_time / val_time (one bar per HTF bar). Buffer rule: slowest EMA period bars before entry on each TF; after entry, candles until trade is completed (exit_time). Implemented in `entry_maps.py`; optional CLI writes JSON to stdout or file. |
| C2 | **Expose** — API or script that calls C1 and returns JSON (e.g. GET /entries?run_id=... or ?run_key=...). Persist to journal table optional. **Run selector:** Frontend can list runs via run_key (and run_id); user chooses run_key; backend resolves to run_id and returns entry maps for that run. |

#### Choosing storage for entry maps (journal)

**Decision:** Entry maps can be built on the fly; no separate journal table is required for C1/C2. Optionally add table `entries` later for caching. Chart buffers (OHLC + ema_slow, ema_medium, ema_fast, atr) are **read from `aligned_candles`** when building entry maps — indicators computed once at start of B, no recomputation in C.

---

### Phase D: Frontend — Review + discretion

**Run:** Start C2 API (`uvicorn api:app --port 8000` or `python3 api.py`), then run frontend (`npm run dev` in frontend directory). **Docker:** From `entry-mgmt-automation`, run `docker compose up --build` to run API and frontend in containers; frontend at http://localhost:80, API at http://localhost:8000. Mount `./data` and `./config.yaml` for DB and config.

| Step | Description |
|------|-------------|
| D1 | **App shell** — React app; fetch runs and entry maps from backend. **Run selector:** User sets **run_id in the interface** (e.g. dropdown of available runs) to change which scan’s entries are shown. Default can be “latest run”. |
| D2 | **Table/cards** — Per symbol + chart TF: show context (C: TF, green/black), validation (V: TF, green/black), status (state, setup time, entry time, entry, SL, TP, BE, **RR, exitReason**). Data filtered by selected run_id. |
| D3 | **Chart** — Lightweight Charts; on selecting a trade row, use buffers already included in that entry map. **D3.1 decision:** start with **Chart TF only**, draw **candles only** (no EMA/ATR lines yet), and default to **option (a): show candles up to entryTime** (not full trade). Option (b) (full trade candles) can be added later via toggle. Optional later: entry/SL/TP price lines. |
| D4 | **Discretion** — Filters (time of day, day of week, session, symbol, etc.) and **pick** trades (e.g. checkboxes / “Include”). Subset P = picked trades. |
| D5 | **Export** — Export P to CSV: symbol, chartTF, entryTime, entryPrice, setupTime, sl, tp, exitReason, rr, etc. |

**Backend API (for run selector and entries):** Expose e.g. `GET /runs` (list of run_id, scan_from, scan_to, created_at) and `GET /entries?run_id=...` (entry maps for that run). Frontend calls these when user changes the run in the UI.

### Phase E: Exit rule optimiser

**Run:** Script TBD; document here when implemented.

| Step | Description |
|------|-------------|
| E1 | **Input** — CSV of fixed entries (from export) + OHLC for each (from Oanda or backend by symbol/TF/entryTime). |
| E2 | **Simulation** — For each entry, bar-by-bar after entryTime: check TP hit, SL hit, BE trigger then BE exit (same rules as Pine). |
| E3 | **Optimisation** — Vary SL/BE/TP (e.g. ATR multiplier, R for BE, R for TP). Score each rule set (e.g. PnL, win rate, RR). Output best or Pareto rule set. |

### Phase F: Entry rule optimiser

**Run:** Script TBD; document here when implemented.

| Step | Description |
|------|-------------|
| F1 | **Input** — CSV of picked trades P (symbol, chartTF, entryTime, entryPrice) + Oanda history (same range as scan). |
| F2 | **Param sweep** — Run scanner with many entry-rule param sets (e.g. maxCandlesAfterBreak, maxDistanceFromEMA, EMA periods). Each run yields an entry set. |
| F3 | **Scoring** — Compare each param set’s entry set to P (e.g. recall, F1). Choose entry rules that best match your picks. |

### Phase G: Apply results

**Run:** Manual/config update (or script TBD); document here if automated.

| Step | Description |
|------|-------------|
| G1 | **Exit rules** — Update Pine (or backend) with chosen SL/BE/TP from exit optimiser. |
| G2 | **Entry rules** — Optionally update scanner config with best entry params; re-scan and repeat review/export/optimise when refining. |

---

## 3. Data shapes (reference)

**Entry map (per trade):**

- Identity: `symbol`, `chartTF`, `contextTF`, `validationTF`, **`run_id`**
- Context: `contextBullish`
- Validation: `validationOk`
- Status: `state`, `setupTime`, `entryTime`, `exitTime`, `entryDay`, `entryPrice`, `sl`, `tp`, `slSize`, `beActive`, `rr`, `exitReason`
- **Chart buffer:** `[{ time, open, high, low, close, ema_slow?, ema_medium?, ema_fast?, atr? }, ...]` (OHLC + indicator series for frontend charts; UTC)

**Export CSV (picked trades):**  
`symbol`, `chartTF`, `entryTime`, `entryPrice`, `setupTime`, `sl`, `tp`, `exitReason`, `rr`, … (whatever exit/entry optimisers need).

---

## 4. Notes

- **When adding a new runnable stage:** (1) Add a row to the "Running the pipeline (script order)" table in §1 with Order, Phase/Stage, Script, and Notes. (2) Set or update the **Run:** line in that phase’s section so the script is documented in one place.
- **Full trade for UI:** Scanner runs full trade (with default exit rules) so frontend can show result (TP/SL/BE, RR) for discretionary review.
- **Charts:** Lightweight Charts (no Advanced Charts for personal use); OHLC and indicator series (ema_slow, ema_medium, ema_fast, atr) come from the entry map buffer, read from `aligned_candles` (indicators stored at start of B).
- **Pine reference:** Logic and state machine follow `entry_mgmt.pine` and [entry_mgmt-architecture.md](../entry_mgmt-architecture.md).
- **Implemented (Phase A):** A2 writes aligned candle data to SQLite table `aligned_candles` (path `data/trendfinder.db` by default; schema includes `symbol`, `chart_tf`, `time`). Phase B will read from this table and write to `scan_runs` + `raw_trades`; Phase C and frontend consume by run_id.
