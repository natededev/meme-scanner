meme-radar
==========

A small, dependency-light Solana **new-pool scanner**. It polls a public DEX API
every 30 seconds, detects pools it has never seen before, records them in a local
SQLite ledger and announces each one on the console together with **how many
minutes old it was when first seen**. Each stored pool is then re-measured once
at 5m, 15m, 1h and 4h of age (a "snapshot"), and `report.py` turns those
snapshots into median market-cap drift and mortality per milestone.

```
2026-09-29 12:31:05 INFO    meme-radar 0.3.0 starting (source=geckoterminal)
2026-09-29 12:31:05 INFO    ledger meme_radar.db already holds 0 pair(s) from 0 token(s)
2026-09-29 12:31:05 INFO    snapshots enabled: 5m/15m/1h/4h (grace 15 min, 30 addresses per request)
2026-09-29 12:31:05 INFO    polling solana via geckoterminal every 30s (db=meme_radar.db, snapshots=on)
[NEW] $LIES.LIES  dex=pump-fun  age=0.4m  liq=$2.67K  mcap=$6.73K  vol5m=$7.14  pair=9WzVkpvGAYquXjGhcAN6Z2aK1DznKZR3AqS9B9UfFmCq  created=2026-09-29T11:55:02Z  first_seen=2026-09-29T11:55:26Z
2026-09-29 12:31:08 INFO    pass complete: 1 new pair(s)
2026-09-29 12:35:09 INFO    snapshots: 12 row(s) written for 2 label(s)
```

## Discovery sources

`--source` selects where the new pools come from; both write to the same schema.

| `--source` | Default | Endpoints | Rate limit |
| --- | --- | --- | --- |
| `geckoterminal` | yes | `GET /api/v2/networks/solana/new_pools` (paged, 20 pools/page) | 30 req/min |
| `dexscreener` | | `GET /token-profiles/latest/v1` + `GET /token-pairs/v1/solana/{mint}` | 60 / 300 req/min |

GeckoTerminal's `new_pools` feed already carries the metrics, so it needs one
request per page; the DexScreener path asks for the pools of every token that
recently entered the profile feed (several requests, but it surfaces tokens
whose profile precedes or lacks a GeckoTerminal listing).

## Files

| File | Purpose |
| --- | --- |
| `scanner.py` | The scanner (discovery, snapshots, rate limiting, DB writes, CLI). |
| `report.py` | Reads the ledger and prints market-cap drift / mortality per label. |
| `requirements.txt` | Runtime dependency (`httpx`). |
| `.gitignore` | Ignores virtualenvs, caches and the local `*.db` ledger. |
| `meme_radar.db` | SQLite database, created automatically on first run. |

## Install

```powershell
cd c:\Users\ADMIN\projects\meme-scanner
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Python 3.9+ (developed and verified against Python 3.13).

## Usage

```powershell
python scanner.py                                    # geckoterminal, 30s loop, snapshots on
python scanner.py --once                             # single pass, prints what it found, exits
python scanner.py --source dexscreener               # use the DexScreener discovery path
python scanner.py --pages 3 --interval 60            # scan 60 new pools per pass
python scanner.py --min-liquidity 5000 --max-pair-age-hours 6
python scanner.py --no-snapshots                     # discovery only
python report.py                                     # median drift + mortality per label
python report.py --label 1h --csv                    # one label, machine readable
```

Stop the loop with `Ctrl+C`; the current pass finishes and the connection to
SQLite and the HTTP client are closed cleanly.

### Options

| Option | Default | Description |
| --- | --- | --- |
| `--source {geckoterminal,dexscreener}` | `geckoterminal` | Discovery source. |
| `--interval SECONDS` | `30` | Delay between passes. |
| `--once` | off | Run a single pass and exit. |
| `--db PATH` | `meme_radar.db` | SQLite file to write to. |
| `--pages N` | `1` | `geckoterminal`: `new_pools` pages per pass (20 pools each). |
| `--max-tokens-per-scan N` | `25` | `dexscreener`: token lookups per pass (bounds API usage). |
| `--min-liquidity USD` | `0` | Skip pools with less than this liquidity. |
| `--max-pair-age-hours H` | `24` | Skip pools created longer ago than this (`0` disables the check). |
| `--snapshots / --no-snapshots` | on | Record the 5m/15m/1h/4h milestone snapshots. |
| `--snapshot-grace-minutes M` | `15` | Drop a milestone that is only noticed this long after it was due. |
| `--timeout SECONDS` | `10` | Per-request HTTP timeout. |
| `--max-attempts N` | `4` | Attempts per request before giving up. |
| `--log-level LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

## How it works

1. **Discovery** – the selected source returns the pools it currently considers
   new:
   * `geckoterminal`: `GET /api/v2/networks/solana/new_pools?page=N`, newest
     first, 20 pools per page (`--pages`); one request per page, no follow-up
     call is needed because the metrics ride along.
   * `dexscreener`: `GET /token-profiles/latest/v1`, keep the `chainId ==
     "solana"` entries not yet in the `tokens` table, then
     `GET /token-pairs/v1/solana/{mint}` per token.
2. **Mapping** – each source turns its payload into a `NewPair`
   (see the field mapping table below).
3. **Filtering** – pools older than `--max-pair-age-hours` (via
   `pool_created_at` / `pairCreatedAt`) or below `--min-liquidity` are dropped.
4. **Storage** – every unseen pool address is inserted into `pairs` and printed
   with its age in minutes. `INSERT OR IGNORE` on the primary key means a pool is
   never stored twice, even across restarts.
5. **Sleep** – the loop waits `interval - elapsed` seconds, so the cadence stays
   at ~30s regardless of how long the API calls took.
6. **Milestones** – pools whose age just passed 5m/15m/1h/4h are looked up in
   batches of 30 through the multi-pools endpoint and written to `snapshots`
   (see *Milestone snapshots* below).

### Field mapping

| Column | GeckoTerminal | DexScreener |
| --- | --- | --- |
| `pair_address` | `attributes.address` (fallback: `id` minus the `solana_` prefix) | `pairAddress` |
| `token` | base side of `attributes.name` (`"DMC / SOL"` → `DMC`) | `baseToken.symbol` |
| `first_seen` | moment the scanner saw it (UTC) | same |
| `liquidity_usd` | `attributes.reserve_in_usd` | `liquidity.usd` |
| `market_cap` | `market_cap_usd`, falling back to `fdv_usd` | `marketCap`, falling back to `fdv` |
| `volume_5m` | `volume_usd.m5` | `volume.m5` |
| `token_address` | `relationships.base_token.data.id` minus `solana_` | `baseToken.address` |
| `pair_created_at` | `attributes.pool_created_at` (stored as UTC `…Z`) | `pairCreatedAt` (epoch ms) |
| `dex_id` | `relationships.dex.data.id` (`pump-fun`, `meteora-damm-v2`, …) | `dexId` |

### Database schema

```sql
CREATE TABLE pairs (
    pair_address    TEXT PRIMARY KEY,  -- pool address
    token           TEXT NOT NULL,     -- base token symbol, e.g. "DMC"
    first_seen      TEXT NOT NULL,     -- ISO-8601 UTC, when the scanner saw it
    liquidity_usd   REAL,              -- reserve_in_usd / liquidity.usd
    market_cap      REAL,              -- market_cap_usd / marketCap (fallback: fdv)
    volume_5m       REAL,              -- rolling 5 minute volume
    token_address   TEXT,              -- base token mint (extra, for lookups)
    pair_created_at TEXT,              -- ISO-8601 UTC, pool creation time
    dex_id          TEXT               -- e.g. "pump-fun", "raydium"
);

CREATE TABLE tokens (
    token_address TEXT PRIMARY KEY,    -- only used by the dexscreener source
    first_seen    TEXT NOT NULL,
    checks        INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_pairs_first_seen    ON pairs (first_seen DESC);
CREATE INDEX idx_pairs_token_address ON pairs (token_address);

CREATE TABLE snapshots (
    pair_address  TEXT NOT NULL,       -- the pairs.pair_address being measured
    label         TEXT NOT NULL,       -- '5m' | '15m' | '1h' | '4h'
    taken_at      TEXT NOT NULL,       -- ISO-8601 UTC, when the milestone was read
    price_usd     REAL,                -- base_token_price_usd
    liquidity_usd REAL,                -- reserve_in_usd
    market_cap    REAL,                -- market_cap_usd, falling back to fdv_usd
    volume_5m     REAL,                -- volume_usd.m5
    status        TEXT NOT NULL DEFAULT 'alive',  -- 'dead' when the pool vanished
    PRIMARY KEY (pair_address, label)  -- exactly one snapshot per pool and milestone
);

CREATE INDEX idx_snapshots_label ON snapshots (label, status);
```

Existing ledgers are upgraded automatically: the tables are created with
`CREATE TABLE IF NOT EXISTS`, so a database written by an earlier version gains
`snapshots` on its next run.

The six columns named in the project brief are `pair_address`, `token`,
`first_seen`, `liquidity_usd`, `market_cap` and `volume_5m`; `token_address`,
`pair_created_at` and `dex_id` are extras that the console output and later
analysis need. Pool creation time lives in `pair_created_at`, so the age printed
on the console is reproducible straight from SQL:

```sql
SELECT token, pair_created_at, first_seen,
       ROUND((julianday(first_seen) - julianday(pair_created_at)) * 1440, 1) AS age_minutes
FROM pairs
ORDER BY first_seen DESC
LIMIT 10;
```

`first_seen` and `pair_created_at` are stored in UTC (`…Z`), while the log
prefixes use the machine's local time.

Handy queries:

```powershell
# 10 most recent pools with their age in minutes
python -c "import sqlite3; print(*sqlite3.connect('meme_radar.db').execute('SELECT first_seen, token, ROUND((julianday(first_seen)-julianday(pair_created_at))*1440,1), liquidity_usd, market_cap, pair_address FROM pairs ORDER BY first_seen DESC LIMIT 10'), sep='\n')"

# how many pools are stored
python -c "import sqlite3; print(sqlite3.connect('meme_radar.db').execute('SELECT COUNT(*) FROM pairs').fetchone()[0])"
```

## Milestone snapshots

Every stored pool is re-measured once at each milestone age, so a cohort can be
followed after 5 minutes, 15 minutes, one hour and four hours:

| label | taken once the pool is | still taken no later than (`--snapshot-grace-minutes 15`) |
| --- | --- | --- |
| `5m` | 5 minutes old | 20 minutes old |
| `15m` | 15 minutes old | 30 minutes old |
| `1h` | 1 hour old | 1h 15m old |
| `4h` | 4 hours old | 4h 15m old |

* **Batched** – the due pools of a label are fetched through GeckoTerminal's
  `GET /networks/solana/pools/multi/{addresses}` … up to **30 addresses per
  request** (`chunked()` splits larger sets). That endpoint shares the client's
  single 30 calls/minute limiter with the new-pools feed.
* **Once per label** – `(pair_address, label)` is the primary key, so a snapshot
  is written exactly once and never overwritten, even across restarts.
* **Dead pools** – GeckoTerminal replies `200` and silently omits pools it no
  longer knows, so an address missing from the response is stored with
  `status = 'dead'` and NULL numbers.
* **Never guessed** – if a batch fails (429, 5xx, network, unreadable payload) its
  addresses stay *unresolved* and are retried on the next pass; they are never
  recorded as dead.
* **Grace window** – a milestone is only taken while the pool is still inside the
  grace window. If the scanner was offline, the missed milestone is skipped
  rather than backfilled with data that would be mislabelled as "5m".
* **After discovery** – snapshots run at the end of a pass, so rate-limited
  lookups can never delay seeing brand new pools.

## Reporting with `report.py`

```powershell
python report.py                      # every label
python report.py --label 5m           # a single milestone
python report.py --csv > drift.csv    # raw numbers for a spreadsheet
```

```
meme-radar snapshot report -- meme_radar.db
4,812 pairs stored | 9,120 snapshots | 1,240 dead at their label

label  snapshots  alive  dead  mortality  median mcap  median chg  mean chg
--------------------------------------------------------------------------
5m     2,884      2,501  383     13.3%      $61.40K       -8.4%      -6.1%
15m    2,410      1,982  428     17.8%      $54.70K      -16.2%     -12.9%
1h     2,102      1,318  784     37.3%      $31.05K      -41.7%     -33.5%
4h     1,724        579  1,145   66.4%      $12.88K      -78.9%     -64.2%
```

* **median chg / mean chg** – percentage change of the pool's market cap at that
  label versus its first-seen value in `pairs`; dead pools are excluded from these
  two columns (their market cap is unknown, not zero) but counted separately.
* **dead / mortality** – how many pools had already vanished when that milestone
  was due.
* **median mcap** – median market cap of the alive snapshots at that label.
* Pairs with no first-seen market cap are counted and mentioned below the table,
  and left out of the change columns.
* `--csv` writes raw numbers (`label,snapshots,alive,dead,mortality_pct,
  median_market_cap,median_change_pct,mean_change_pct`).

## Error handling and rate limits

* **Client-side throttling** – one `RateLimiter` per endpoint family keeps the
  scanner inside the published budgets (GeckoTerminal 30 req/min for
  `new_pools`; DexScreener 60 req/min for token profiles and 300 req/min for
  token pairs) by spacing requests out, so it does not provoke 429s.
* **HTTP 429** – the `Retry-After` header is honoured when present (capped at
  120s). Without that header the scanner assumes the host wants real breathing
  room and cools down **the whole host**: 20s, then 40s, 80s, doubling up to
  120s. The streak is counted per client rather than per request, so a second
  rate-limited response inside the same pass doubles again instead of restarting
  at 20s, and it resets only once the API answers. Every later call - other
  endpoints and the remaining snapshot batches of the pass included - waits the
  cooldown out first, so a rate-limited pass parks instead of going back for more
  a second or two later. The endpoint's limiter is *penalised* as well, so the
  per-endpoint budgets keep working as before.
* **HTTP 5xx, timeouts, connection resets** (`httpx.RequestError`) – retried up
  to `--max-attempts` with exponential backoff and jitter; the pass then
  continues with the next item.
* **HTTP 404 and other 4xx** – logged and skipped; a 404 is normal for a token
  whose pool is not indexed yet.
* **Malformed JSON or an unexpected payload shape** – logged and skipped; a bad
  response never crashes the loop.
* **Partial GeckoTerminal page failures** – if page 1 fails the pass is reported
  as failed (`None`) and nothing is recorded; if a *later* page fails, the pools
  read so far are still stored and a warning is logged.
* **Snapshot batches** – a failed `pools/multi` request leaves its addresses
  unresolved and they are retried on the next pass; nothing is ever marked dead on
  a failed request. If the whole snapshot stage raises, it is logged with a
  traceback and the discovery result of the pass is preserved.
* **Console encoding** – stdout/stderr are reconfigured with `errors="replace"`,
  so an emoji-heavy ticker can no longer abort a pass on a cp1252 console.
* **A failed DexScreener token fetch is not marked as seen**, so the token is
  retried on the next pass; a token that legitimately returns zero pools is
  marked as seen.
* **Unexpected exceptions inside a pass** are caught per cycle, logged with a
  traceback and the loop keeps running.
* **SQLite** – WAL journal mode plus a 5s busy timeout, so other tools can read
  the database while the scanner writes.
* **Ctrl+C / SIGTERM** – the loop stops after the current pass, then both the
  database connection and the HTTP client are closed.

## Known limitations

* `new_pools` is a *recently created pools* feed, not a firehose: it returns the
  newest 20 pools per page and only `--pages` pages are read per pass. Pools
  created and drained between two passes can be missed; for sub-second coverage
  use the WebSocket APIs or a Solana RPC/Geyser stream instead of polling.
* GeckoTerminal does not inline token symbols, so `token` is parsed from the pool
  name (`"<base> / <quote>"`) — a symbol containing a slash is cut at the first
  one. Use `token_address` when you need an exact identifier.
* GeckoTerminal's `reserve_in_usd` is a reserve value rather than DexScreener's
  `liquidity.usd`, and it is `null` for many brand-new pump.fun pools, so
  `liquidity_usd` can be `NULL` (printed as `n/a`).
* `first_seen` is when *this scanner* first observed the pool and is later than
  the pool's creation time — the difference is exactly the age printed on the
  console, and it grows with the `--interval` (a 30s loop sees pools seconds
  after creation, a 10min loop sees them up to 10min late).
* Snapshots need the pool to still be listed by **GeckoTerminal**, whichever
  source discovered it, because `pools/multi` is a GeckoTerminal endpoint. A pool
  that GeckoTerminal never indexed is recorded as dead even if it is trading
  elsewhere — that is the definition of "no longer found" used here.
* Milestones are only as punctual as `--interval` (a `5m` snapshot lands within
  30s of the milestone on the default loop), and a milestone missed during an
  outage is skipped rather than backfilled, so the per-label cohort can be smaller
  than the number of stored pairs.
* Snapshots are one reading per milestone: the scanner does not track highs or
  lows in between, and `market_cap` still falls back to `fdv_usd` for pools whose
  market cap is not published.
* The two sources do not cover exactly the same pools and never de-duplicate
  against each other: use one `--db` per source, or accept that `first_seen`
  reflects whichever source spotted a pool first.
* Only Solana is scanned (`CHAIN_ID`; GeckoTerminal network name and DexScreener
  chain id share the string).
* Stored metrics are a first-seen snapshot — the scanner does not track how
  liquidity/volume develop afterwards.
* No API key is required for either API today; add a header in
  `ApiClient.__init__` if that changes.
* Not financial advice — brand new Solana pools are frequently scams or rug
  pulls, and the numbers reported by the APIs can be manipulated.


