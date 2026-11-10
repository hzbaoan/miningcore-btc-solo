# MiningcoreBtcSolo

BTC-only **true solo** Stratum V1 gateway for Bitcoin Core.

It pulls templates from `bitcoind` (`getblocktemplate` + longpoll), optionally refreshes via ZMQ / P2P, validates shares locally, assembles full blocks, and calls `submitblock`. Coinbase always pays a **fixed address** from config — no pool accounts, no database, no SV2, no multi-coin.

## Scope

**Good fit when you:**

- Already run Bitcoin Core and want a local solo Stratum entrypoint
- Want share validation and block submission on your machine (not an upstream pool)
- Need a small .NET 10 process with an embedded read-only dashboard

**Does not:**

| Feature | Status |
|---------|--------|
| Pool payout / PPLNS / accounts | Not implemented |
| Username → payout address | Username is worker label only |
| Stratum V2 | Not implemented |
| Multi-coin | BTC only |
| Persistent DB | In-memory metrics only (pending blocks on disk only if submit fails) |

## Features

- `getblocktemplate` + **longpoll** (primary steady-state path)
- Optional **ZMQ** `hashblock` / `rawblock`
- Optional **P2P fast peer** empty-block clean jobs (mainnet only; inbound P2P payload checksums are skipped, with header and chain safety guards)
- **Chain guard** at startup: refuses to run if `getblockchaininfo.chain` ≠ `network`
- Local share + network-target checks; full block hex assembly before `submitblock`
- **Submit-first queue**: memory enqueue → `submitblock` ASAP; `null` / `duplicate` = success; `inconclusive` is persisted immediately and retried; `prev-blk-not-found` waits for authoritative GBT / chain advance and retries; transport failures are persisted after retry exhaustion; valid orphaned `.json.tmp` files are recovered after restart; permanent consensus rejects → `data/failed-blocks/`
- Stratum V1: subscribe, authorize, submit, configure (version-rolling), suggest_difficulty, extranonce.subscribe, ping
- Per-connection work assignments are bounded; capacity failure preserves old advertised work instead of evicting it
- Version rolling mask capped at `1fffe000`; `vbrequired` bits never rolled
- Segwit coinbase commitment when GBT provides `default_witness_commitment`
- Time-driven VarDiff with zero-share recovery, bounded burst catch-up, immutable old-work targets, and difficulty changes applied on the next public template
- Dashboard + JSON API: `/`, `/api/stats`, `/api/workers`, `/api/shares`, `/api/blocks`, `/healthz`, `/readyz`; UUID-protected runtime P2P peer management
- Greppable `ALERT` logs for found blocks and submit failures
- Networks: `mainnet` (`bitcoin` alias), `testnet`, `regtest`; unsupported networks fail closed
- Regtest harness: empty block + multi-tx mempool + full Stratum path

## Requirements

| Component | Notes |
|-----------|--------|
| .NET 10 SDK | Local build / publish |
| Bitcoin Core | RPC required; ZMQ recommended; P2P optional |
| Linux/Windows | Gateway is pure managed code (+ NetMQ for ZMQ) |

## Quick start

### 1. Configure

Edit [`config.json`](config.json):

```json
{
  "network": "mainnet",
  "bitcoind": {
    "rpc_url": "http://127.0.0.1:8332",
    "rpc_user": "your-rpc-user",
    "rpc_password": "your-rpc-password",
    "zmq_block_urls": ["tcp://127.0.0.1:28332"],
    "p2p_fast_peer": "127.0.0.1:8333"
  },
  "coinbase": {
    "address": "bc1q...your-mainnet-payout...",
    "message": "mcore-solo",
    "segwit_commitment": true
  },
  "difficulty": {
    "min": 1024,
    "max": 1000000000000,
    "default": 8192,
    "target_time_secs": 5
  },
  "runtime": {
    "max_retained_transaction_bytes": 67108864,
    "data_dir": "data"
  },
  "api": {
    "p2p_fast_peer_uuid": "REPLACE_WITH_A_PRIVATE_UUID"
  }
}
```

Replace the RPC credentials and `coinbase.address` before use; those required fields fail startup validation while their `REPLACE_` values remain. The payout address must match `network`. Replace the management UUID placeholder with a valid private UUID; a configured non-UUID fails validation. Remove that setting entirely to leave management disabled.

Set `api.p2p_fast_peer_uuid` to a private UUID and visit `http://127.0.0.1:7152/UUID` to manage peers. The configured `bitcoind.p2p_fast_peer`, manually added peers, and automatically discovered peers run as independent high-bandwidth peers and compete to publish fast jobs. The page stores manual peers in `runtime.data_dir/p2p-fast-peers.json`, automatic-pool settings/blacklist/winner history in `runtime.data_dir/p2p-fast-auto.json`, and independently learned candidates in `runtime.data_dir/p2p-fast-addresses.json`; retain `runtime.data_dir` on persistent storage.

On mainnet, configuring the management UUID starts the automatic peer pool by default. It discovers public candidates through independent P2P `getaddr` sessions, targets 30 automatic peers, and rotates peers that do not win a P2P-fast job within 24 hours. Removed peers enter the persisted blacklist until manually added again or the blacklist is cleared. The page can disable the pool or change its target (1-128) and inactivity window (1-168 hours). Candidate selection prefers past fast-job winners and recent address records; each peer's TCP connect time provides a gateway-local latency sample after connection. Automatic peers are capped separately from the configured and manual peers. The dashboard rank counts each peer's successful publications of a new P2P-fast job; it is observed performance for this node's topology, not a network-wide latency guarantee. Automatic discovery is unavailable on test networks. Without the UUID, peer management and automatic discovery are disabled while the configured single peer continues to work. Without either the UUID or `bitcoind.p2p_fast_peer`, P2P-fast is disabled.

Address discovery runs every five minutes on separate short-lived sockets, with
at most two concurrent connections and one `getaddr` request per connection.
It negotiates `sendaddrv2` before `verack` on modern peers and accepts legacy
`addr` responses as well as BIP155 `addrv2`. Configured/manual/current peers and
the persistent candidate cache provide starting addresses. When those sources
do not return usable candidates, multiple Bitcoin DNS seeds supply bootstrap
IPs on port 8333; bootstrap IPs must still return addresses over P2P. Discovery
does not call Core's `getpeerinfo` or `getnodeaddresses` RPCs.

Each round reserves queries for independently learned addresses so discovery
can extend beyond the current mining peer set. Successful sources have a
30-minute request cooldown, failed sources five minutes, and DNS queries a
30-minute cooldown. Sessions time out after eight seconds. The cache retains at
most 4,096 candidates for 30 days. Address messages have checksum, count,
CompactSize, length, service-bit and public-IP checks; IPv4 and IPv6 actual ports
are retained and unsupported address networks are skipped. These are candidate
addresses, not proof of an available or fast mainnet node: production P2P
connections and validated block announcements determine their usefulness.

Use **固定** on an automatically discovered peer to move its existing runtime connection into the persistent manual peer list, where automatic inactivity rotation no longer manages it. **清空黑名单** removes all blacklist entries and lets enabled automatic discovery consider those addresses again.

### 2. Bitcoin Core

```ini
server=1
rpcuser=your-rpc-user
rpcpassword=your-rpc-password
rpcallowip=127.0.0.1
zmqpubhashblock=tcp://127.0.0.1:28332
zmqpubrawblock=tcp://127.0.0.1:28333
```

Keep RPC **private**. ZMQ is optional but recommended for faster tip switch.

### 3a. Run from source

```bash
dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
./build/MiningcoreBtcSolo --config config.json
```

### 3b. Docker Compose

```bash
# config.json is injected at runtime and is excluded from the image build context
docker compose up -d --build
```

Default ports: **Stratum `3333`**, **Dashboard `7152`**.

Compose mounts `./config.json` read-only at runtime; the image contains no configuration file or RPC credentials. It uses the Docker-managed `miningcore-btc-solo-data` volume at `/app/data` for submit-failure recovery and `p2p-fast-peers.json`, with ownership initialized for the image's non-root UID 10001. On Linux, compose uses `network_mode: host` so the gateway can reach `bitcoind` on localhost. On Docker Desktop (Windows/macOS), switch to bridge networking and publish ports (see comments in `docker-compose.yml`).

### 3c. Prebuilt image (GHCR)

After publishing via GitHub Actions:

```bash
docker pull ghcr.io/<owner>/<repo>:latest
docker volume create miningcore-btc-solo-data
docker run --rm --network host \
  -v "$PWD/config.json:/app/config.json:ro" \
  -v "miningcore-btc-solo-data:/app/data" \
  -e SOLO_CONFIG_PATH=/app/config.json \
  ghcr.io/<owner>/<repo>:latest
```

Tags: `latest`, `sha-<short>`, and version tags from `v*` releases.

## Configuration reference

| Key | Description |
|-----|-------------|
| `network` | `mainnet` \| `bitcoin` \| `testnet` \| `regtest` |
| `log_level` | Console log level (default `Information`) |
| `stratum.listen_addr` / `listen_port` | Stratum bind (secure sample default `127.0.0.1:3333`; widen explicitly for remote miners) |
| `stratum.extranonce1_size` / `extranonce2_size` | 1–4 / 1–8 bytes |
| `stratum.idle_timeout_secs` | Handshake timeout for connections that have not completed subscribe + authorize; authorized miners are exempt (code default 3600, mainnet sample 300) |
| `stratum.max_connections` | Max concurrent miners (default and tested mainnet sample 256) |
| `stratum.max_message_bytes` | Maximum UTF-8 bytes in one newline-delimited request (default 65536) |
| `stratum.send_queue_capacity` | Bounded outbound frames per miner; full queues disconnect slow clients (default 64) |
| `stratum.write_timeout_secs` | Per-frame socket write timeout before disconnect (default 10) |
| `stratum.clean_broadcast_timeout_ms` | Deadline for a clean job to reach every live miner's TCP stack; laggards disconnect (default 1500) |
| `stratum.late_share_grace_ms` | Retain clean-retired jobs after delivery for shares already in flight (default 2000) |
| `bitcoind.rpc_*` | JSON-RPC endpoint and Basic auth |
| `bitcoind.zmq_block_urls` | List of ZMQ endpoints (optional) |
| `bitcoind.p2p_fast_peer` | Single `host` or `host:port` peer (optional; always active and included alongside runtime peers when UUID management is enabled) |
| `coinbase.address` | Fixed payout; must match `network` |
| `coinbase.message` | Coinbase push message (ASCII) |
| `coinbase.segwit_commitment` | Include BIP141 commitment from GBT |
| `difficulty.min` / `max` / `default` | Share difficulty bounds (raise `max` for multi-TH/PH); the effective value is always clamped below the current network difficulty |
| `difficulty.target_time_secs` | Desired seconds between accepted shares per connection |
| `difficulty.retarget_time_secs` | Steady VarDiff interval; zero-share windows lower difficulty (default 30) |
| `difficulty.retarget_share_burst` | Early upward retarget after N accepted shares; work estimate is difficulty-weighted (default 8) |
| `difficulty.variance_percent` | Stable share-interval band that does not retarget (default 30%) |
| `difficulty.retarget_smoothing` | New-window EWMA weight; lower values reduce difficulty oscillation (default 0.25) |
| `difficulty.max_step_up` / `max_step_up_burst` | Steady vs flood max × multiplier (2 / 32) |
| `difficulty.max_step_down` | Max downward × step (default 0.5) |
| `runtime.keep_old_jobs` | Job cache for late submits |
| `runtime.max_retired_jobs` | Hard memory bound for clean-retired full templates (default 8) |
| `runtime.retired_job_max_age_secs` | Hard retirement age if delivery/grace tracking cannot complete (default 15) |
| `runtime.max_retained_transaction_bytes` | Transaction byte budget for retained templates; active job is never reclaimed (default 64 MiB) |
| `runtime.data_dir` | Persistent root for pending/failed blocks and P2P peer state files (default `data/`) |
| `api.*` | Dashboard bind / enable; `p2p_fast_peer_uuid` enables peer management; bind to loopback unless protected by a trusted VPN or HTTPS reverse proxy |

Work-token memory is bounded per connection: at most 256 active public-job
assignments and 256 retired-token lookups are retained, and old epochs are
pruned as template retention advances. A VarDiff decision remains pending until
the next public template atomically carries `mining.set_difficulty` and the job
registered at that target. If a public job cannot register an assignment, that
miner session is disconnected instead of evicting already advertised work. With
`stratum.max_connections: 256`, aggregate work-token state is also bounded.

`stratum.idle_timeout_secs` is intentionally only a pre-authorization handshake
timeout. When such an idle socket is closed, the connection is removed from the
server and its bounded session state becomes eligible for garbage collection.
An authorized miner may remain read-idle indefinitely because Stratum V1 has no
required heartbeat; its state is released on disconnect, write failure, or
server shutdown.

Environment: `SOLO_CONFIG_PATH` overrides config path. CLI: `--config` / `-c`.

### Networks

| Value | Address validation | Default P2P port | P2P empty clean job |
|-------|--------------------|------------------|---------------------|
| `mainnet` / `bitcoin` | Main | 8333 | Yes (guarded) |
| `testnet` | TestNet | 18333 | No |
| `regtest` | RegTest | 18444 | No |

P2P empty-job guards (mainnet): reject malformed or oversized `headers` batches, skip retarget height `% 2016 == 0`, skip when `vbrequired != 0`, and require parent linkage, height, compact target, PoW, MTP, future-time, and block-version checks before publishing work.

Startup maps `network` → Core `chain` (`main` / `test` / `regtest`) and **exits** on mismatch. It also exits before requesting GBT or starting Stratum while Core reports IBD. If IBD is false but headers are still ahead of blocks, startup logs a warning and continues.

## How it works

```
bitcoind ──RPC──► TemplateEngine ──notify──► Stratum V1 miners
          │            ▲
          ├── ZMQ ─────┤
          └── P2P ─────┘ (optional empty clean job)

coinbase.scriptPubKey = config.coinbase.address (fixed)
Block found ──► BlockSubmitQueue (memory) ──submitblock──► bitcoind
                     │ (inconclusive immediately; transport failure after retries)
                     └── fsync data/pending-blocks/  (restart recovery)
```

1. **ChainGuard**: `getblockchaininfo.chain` must match `network` (fail closed).
2. Startup GBT + longpoll for tip / mempool template updates.
3. **P2P** (optional): maintain one independent connection per peer, advertise `relay=0`, and request both high-bandwidth compact-block v2 and proactive `headers` via `sendheaders`. Announce the current Core-confirmed tip after the handshake and when it changes, and respond to peer `getheaders` requests from a shared tip-header cache. Publish `cmpctblock`/`block` work as soon as the 80-byte header arrives while draining the rest of the frame. `headers` announcements are parsed as soon as their frame arrives. Peers that use `inv` remain supported through `getheaders`; duplicate requests for the same locator are coalesced for five seconds, after which a later inventory can retry. Incoming payload checksums are intentionally skipped for minimum latency; frame length/truncation and P2P-fast header/chain guards remain.
4. **ZMQ** and **longpoll** both schedule full GBT; first successful apply wins; identical template key → skip (key includes ordered-txid fingerprint).
5. P2P-fast work can be active mining work; a pending VarDiff decision applies to the next public template from P2P-fast, longpoll, or ZMQ without re-pushing the current template.
6. On `mining.submit`, rebuild coinbase + merkle + header; check share and block targets.
7. If the header meets the network target: ack `mining.submit`, enqueue **submitblock ASAP** (no disk on the happy path). `inconclusive` is durable and retried; `prev-blk-not-found` is deferred and retried immediately when authoritative GBT confirms a chain advance, with timed/disk recovery as fallback. Startup recovers valid final `.json` files and orphaned `.json.tmp` writes. Permanent consensus rejects are archived to `failed-blocks/`. Grep logs for `ALERT`.
8. Metrics and recent shares/blocks stay in memory for the dashboard.

P2P tip notification runs independently of each peer's receive loop, so a slow
`getblockheader` RPC cannot delay incoming block headers or heartbeats. Peers
share one cached response per confirmed tip; failed lookups have a bounded
timeout and retry cooldown. Tip changes are checked every two seconds without
re-querying an unchanged cached header. Socket writes serialize complete P2P
frames and have a five-second timeout. A disconnected/truncated stream reports
the message stage and received/expected byte counts. All new parent headers
still pass linkage, target, PoW, time, and version checks before fast publication;
later announcements of an already active tip do not create another job.

### Template refresh priority (solo)

1. **P2P** empty clean job (mainnet, when safe) — no GBT on success path
2. **ZMQ** → full GBT (race with longpoll)
3. **Longpoll** → full GBT (race with ZMQ)
4. Unchanged template key → `skip_job` (no stratum notify)

### Block submit outcomes

| Core result | Gateway behavior |
|-------------|------------------|
| `null` (accepted) | Success; drop pending file if any; refresh template |
| `duplicate` | Success (already on chain / known) |
| `inconclusive` / `duplicate-inconclusive` | Uncertain; persist candidate and retry; never archive as a consensus rejection |
| Other string | Rejected; archive hex under `failed-blocks/` |
| RPC / network error | Retry with backoff; persist to `pending-blocks/` if still failing |

## Regtest validation (pre-mainnet)

Prove **share → full block (incl. mempool txs) → submitblock → active chain** before pointing real hashrate at mainnet.

**Prerequisites:** .NET 10 SDK, Bitcoin Core (`bitcoind` on `PATH` or under `Program Files\Bitcoin\daemon`, or set `BITCOIND`).

```bash
# Windows
scripts\run-regtest-validation.cmd

# any OS
dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
dotnet run --project src/MiningcoreBtcSolo.Regtest -c Release -- all
```

To execute the 12-mode nonredundant pre-mainnet suite, run
`powershell -ExecutionPolicy Bypass -File scripts/run-regtest-validation.ps1 suite`.

| Mode | What it runs |
|------|----------------|
| `all` (default) | Empty block + mempool multi-tx + Stratum multi-tx |
| `direct` | Empty + mempool library paths |
| `mempool` | Multi-tx library path only |
| `stratum` | Gateway + Stratum V1 with mempool txs |
| `vardiff` | Offline VarDiff regression checks; no bitcoind required |
| `encoding` | Offline encoding, reorg, subsidy-halving, and P2P-fast boundary checks |
| `p2p-admin` | Offline UUID/session/peer management, independent `getaddr` + `addr/addrv2`, legacy fallback, malformed-frame rejection, cache/DNS bounds, `sendheaders`, shared tip notification, and slow-RPC receive isolation |
| `shutdown` | Offline submit-queue shutdown persistence check |
| `safety` | Offline public-work capacity, pending-difficulty/source ordering, immutable old-work targets, `vbrequired`, submit temp-file recovery, P2P header read and truncated-frame rejection, ownership, service-failure, readiness, and duplicate-share checks |
| `synthetic-gbt` | Offline 10k/20k transaction GBT JSON parse, fingerprint, Merkle, and job-build stress checks |
| `large-mempool` | Real bitcoind + 10,001 independent mempool txs + published gateway/Stratum/production submit queue + full active-chain txid confirmation |
| `stress` | `synthetic-gbt` followed by `large-mempool` |
| `p2p-fast` | Real Core header/P2P handshake + independent P2P `getaddr` discovery + coinbase-only job + mined block active-chain confirmation |
| `lifecycle` | `p2p-fast` block validation + 16 Stratum miners + rapid clean/late/stale/ZMQ/longpoll lifecycle checks |
| `extranonce` | 4x8 size matrix (32 direct + 32 published-gateway blocks), then `(1,1)/(1,8)/(4,1)/(4,8)` crossed with witness mempool, BIP310, and clean/late/stale lifecycle |
| `core-restart` | Owned local Core offline/restart, `/readyz`, longpoll/ZMQ/P2P reconnect, retry persistence, gateway crash, and pending-block startup recovery |

The `suite` runner executes `vardiff`, `encoding`, `p2p-admin`, `shutdown`, `safety`,
`synthetic-gbt`, `all`, `p2p-fast`, `lifecycle`, `core-restart`, `extranonce`, and
`large-mempool` in fresh processes. `all` covers the `direct`, `mempool`, and `stratum` paths; running
`synthetic-gbt` plus `large-mempool` covers the composite `stress` path. Those
four focused entry points remain available for diagnosis. Unknown mode names
fail with a nonzero exit code. See [VALIDATION.md](VALIDATION.md) for the latest
validation evidence.

CI repeats the published-gateway production paths (`all`, `p2p-fast`,
`lifecycle`, and `extranonce`) against a digest-pinned Bitcoin Core 31.1 regtest
container. The expensive 10,001-transaction case remains part of the local
nonredundant suite. CI intentionally does not run `core-restart`: its Core is an
externally managed Docker process, while the fault test is permitted to stop only
a loopback Core process that the harness itself started and marked as owned.

Modes `vardiff`, `encoding`, `p2p-admin`, `shutdown`, `safety`, and `synthetic-gbt` are offline. Other modes start a local `bitcoind -regtest` if RPC is not already up, verify `chain=regtest` before mutating the fixture, bootstrap a mature test chain, then run the same production `ChainGuard` and require `IBD=false`. They seed bech32 / p2sh-segwit spends, mine via share assembly and/or the published Stratum gateway, and confirm tip hash + txids on the active chain. The `large-mempool` mode creates confirmed fan-out UTXOs, broadcasts 10,001 independent P2WPKH spends, and requires every txid in the gateway-submitted block. `core-restart` refuses an already-running/external RPC node; its PID marker, loopback RPC, and datadir-under-workdir checks must all pass before fault injection.

The harness creates an isolated temporary bitcoind datadir, gateway config, and submit directory. RPC, P2P, and ZMQ bind to loopback; peer discovery, fixed seeds, and NAT port mapping are disabled. Temporary data is removed on exit. Set `REGTEST_WORK_DIR` to retain it for debugging.

Regtest validates the executable, RPC/GBT, ZMQ/P2P refresh, Stratum, share
validation, block assembly, durable submit queue, and active-chain acceptance
paths with real Bitcoin Core. It does not reproduce mainnet proof-of-work
difficulty, network magic/ports, accumulated chain work, peer topology, or IBD
duration. The offline `encoding` and `safety` modes cover mainnet-only constants
and fail-closed policy branches; the go-live checklist remains mandatory.

Build artifacts are **gitignored** (`build/` and project `bin/` / `obj/`). Clean
only those generated paths, for example on PowerShell:

```powershell
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue build,
  src/MiningcoreBtcSolo/bin, src/MiningcoreBtcSolo/obj,
  src/MiningcoreBtcSolo.Regtest/bin, src/MiningcoreBtcSolo.Regtest/obj
```

`data/` is runtime recovery state, not a build artifact. Do not bulk-clean it or
use `git clean -fdX` unless pending/failed block candidates are intentionally
being discarded.

**Mainnet go-live checklist** (after regtest is green):

1. `network: "mainnet"` and a real `coinbase.address` (bech32 recommended)
2. RPC credentials + firewall; RPC/API stay private, and Stratum defaults to loopback. Widen `stratum.listen_addr` only for intended miners and firewall that listener; the gateway refuses start if bitcoind `chain` ≠ config or Core reports IBD
3. ZMQ / optional `p2p_fast_peer`; with UUID management enabled, add runtime peers alongside it
4. `difficulty.min` / `default` / `max` sized for your hashrate
5. Persist `runtime.data_dir` (compose volume `miningcore-btc-solo-data`) for rare submit-failure recovery; monitor `ALERT` and `/api/blocks`
6. Miners connect **only** to this gateway

## API

| Path | Purpose |
|------|---------|
| `GET /` | Dashboard HTML |
| `GET /api/stats` | Height, hashrate (`hashrate_hps`), share/block counters, best share |
| `GET /api/workers` | Connected workers (hashrate, difficulty, shares) |
| `GET /api/shares` | Recent accepted/rejected shares |
| `GET /api/blocks` | Recent submit results (`submitted` / `duplicate` / `inconclusive` / `rejected`) |
| `GET /healthz` | Liveness |
| `GET /readyz` | Template ready + last refresh OK (`503` when not ready) |
| `GET /UUID` | Exchange configured UUID for the P2P management session |
| `GET /api/p2p-fast/peers` | Peer status snapshot (management session required) |
| `GET /api/p2p-fast/auto` / `PUT /api/p2p-fast/auto` | Read/update mainnet automatic pool (`enabled`, `targetPeers`, `inactiveHours`) |
| `DELETE /api/p2p-fast/auto/blacklist` | Clear the persisted peer blacklist |
| `POST /api/p2p-fast/peers` / `DELETE /api/p2p-fast/peers?peer=...` | Add/remove a runtime peer |
| `POST /api/p2p-fast/peers/import` / `GET /api/p2p-fast/peers/export` | Replace/export the JSON peer array |
| `POST /api/p2p-fast/peers/probe?peer=...` | Run a three-sample TCP connect probe |

The UUID entry page provides live add/remove, automatic-peer pinning, blacklist
clearing, three-sample TCP connect probes, and JSON import/export. Adding an
already automatic peer promotes its current runtime connection into the manual
list. Per-attempt connect timeout is 15 seconds; automatic
background probes repeat every 30 minutes. Each peer retains its independent
P2P handshake and reconnect loop. The displayed fast-block count advances only
when that peer's announcement publishes a new P2P-fast job. Automatic peer
rotation runs in the background and does not block peer header handling or fast
job publication.

`/api/stats` exposes SI hashrate as `hashrate_hps` (preferred). `hashrate_th_s` is kept for older clients (`hps / 1e12`).
It also separates job-lifecycle outcomes as `shares_late` (accepted during grace),
`shares_stale` (known but already reclaimed), and `shares_unknown_job`. Clean fan-out
health is exposed as `clean_broadcasts` and `clean_broadcast_client_timeouts`.
Share response timing is split into `share_validation_avg_ms` / `max_ms`, accepted ACK
queue latency, and accepted ACK socket-write latency. `share_accepted_ack_written` only
advances after `NetworkStream.WriteAsync` hands the response to the TCP stack; comparing
it with `share_accepted_ack_queued` reveals responses lost to slow or closed connections.

## CI / container images

| Workflow | Trigger | Action |
|----------|---------|--------|
| [`.github/workflows/ghcr-build.yml`](.github/workflows/ghcr-build.yml) | `workflow_dispatch`, tags `v*` | Build `linux/amd64` image → **GHCR** |

Image name: `ghcr.io/<lowercase-owner>/<lowercase-repo>`

| Tag | When |
|-----|------|
| `latest` | Every successful GHCR build |
| `sha-<short>` | Every successful GHCR build |
| `v1.2.3` / `1.2.3` / `1.2` | Push git tag `v1.2.3` (semver) |

**Publish an image:**

1. Push code to GitHub (enable Actions for the repo).
2. **Actions** → **Build And Push GHCR Image** → **Run workflow**, **or** create and push a tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. Package appears under **Packages** on the repo / org page.

Package visibility: GitHub → Packages → package settings (public/private as needed). First pull of a private package requires:

```bash
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

`GITHUB_TOKEN` is provided automatically to the workflow; no extra secret is required for same-repo GHCR push (needs `packages: write`).

## Repository layout

```
├── config.json                 # mainnet template (edit before production)
├── config.regtest.json         # regtest template
├── config.umbrel.json          # Umbrel-oriented template
├── scripts/                    # regtest one-shot helpers
├── docker-compose.yml
├── docker-compose.umbrel.yml
├── Dockerfile
├── MiningcoreBtcSolo.sln
├── .github/workflows/          # GHCR publish
└── src/
    ├── MiningcoreBtcSolo/      # gateway
    │   ├── Api/                # dashboard + JSON API
    │   ├── Metrics/
    │   ├── P2p/
    │   ├── Rpc/                # BitcoinRpcClient, ChainGuard
    │   ├── Share/
    │   ├── Stratum/
    │   ├── Submit/             # BlockSubmitQueue
    │   ├── Template/
    │   └── Util/
    └── MiningcoreBtcSolo.Regtest/   # pre-mainnet harness
```

## License

MIT
