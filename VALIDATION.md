# Pre-mainnet Validation Report

Status: **PASS** for build, offline safety checks, and isolated regtest.

Validation date: 2026-08-15 (Asia/Shanghai)

Environment:

- Windows 10.0.26200 x64
- .NET SDK 10.0.302, .NET runtime 10.0.10
- Bitcoin Core v31.1.0
- Release configuration

## Build and dependency checks

`dotnet restore MiningcoreBtcSolo.sln` and
`dotnet build MiningcoreBtcSolo.sln -c Release --no-restore` completed with
0 warnings and 0 errors. `dotnet list MiningcoreBtcSolo.sln package --vulnerable
--include-transitive` reported no known vulnerable packages. `git diff --check`
also passed.

The two source files required by tracked callers are included in Git:

- `src/MiningcoreBtcSolo/Rpc/GbtResponseJsonConverter.cs`
- `src/MiningcoreBtcSolo/Share/BlockCandidate.cs`

## Regtest harness

The 11-mode nonredundant suite ran sequentially in isolated temporary datadirs
and returned exit code 0. After stabilizing the lifecycle fixture's test-only
retired-job capacity, `lifecycle` passed five consecutive reruns. `safety` also
passed five consecutive race-focused reruns and a final rerun after configuration
hardening. An unknown mode returned exit code 2.

The earlier 10-mode baseline completed in 577.5 seconds. After adding the three
risk-oriented crosses, focused real-Core reruns completed `core-restart` in 100.6
seconds, `extranonce` in 115.3 seconds, and `large-mempool` in 512.8 seconds.
The final 11-mode suite then passed end to end in 689.5 seconds.

| Mode | Result | Evidence |
|---|---|---|
| `vardiff` | PASS | Deterministic silent-window, burst, weighted old-work, smoothing, and boundary checks |
| `encoding` | PASS | Compact target, BIP34, Merkle, reorg, subsidy-halving, and strict P2P-fast boundary checks |
| `shutdown` | PASS | Undrained and delayed-retry candidates persisted for restart recovery during bounded shutdown |
| `safety` | PASS | Placeholder/default-bind fail-closed checks; IBD fail-fast; BIP310 mask enforcement; pending VarDiff application on public templates with immutable old-work targets; pending-file cleanup before accepted metrics; Core restart/longpoll generation invalidation with the same transaction count but a different transaction set; pending recovery; parser, ownership, checksum, and retirement checks |
| `synthetic-gbt` | PASS | 10,000 and 20,000 transaction JSON parse, fingerprint, Merkle, and job-build cases |
| `all` | PASS | Empty block, direct assembly with 3 witness transactions, Stratum submission, and active-chain confirmation |
| `p2p-fast` | PASS | Regtest coinbase-only block accepted with the configured 50 BTC payout and active-chain confirmation; production network/PoW policy remained strict |
| `lifecycle` | PASS | Five reruns with 16 miners and a 3-block burst; 0.385-13.867 ms final clean spread; every run recorded 16 late, 1 stale, 1 unknown, and 0 client timeouts |
| `extranonce` | PASS | 32 size pairs through `JobBuilder`/`ShareValidator` and the same 32 through the published gateway (64 accepted blocks), plus boundary pairs `(1,1)/(1,8)/(4,1)/(4,8)` crossed with two witness sets, BIP310, 4-miner clean/late/stale, and on-chain txid checks |
| `core-restart` | PASS | Owned-Core offline/restart kept `/healthz=200`, changed `/readyz` 503 to 200, observed longpoll failure plus ZMQ/P2P reconnect, persisted a candidate after the unchanged production retry schedule, killed/restarted the gateway, and recovered the pending block onto the active chain |
| `large-mempool` | PASS | The published gateway exposed all 10,001 transactions over Stratum, accepted `mining.submit`, sent the 10,002-transaction candidate through production `BlockSubmitQueue`, confirmed every txid on chain, and left mempool/pending/failed empty; block weight was 3,731,112 WU |

The final `safety` rerun covered the 3-by-3 Longpoll, ZMQ, and P2P-fast public
template source matrix. Each pair retained ordinary job IDs `1` through `4`,
applied only the latest pending difficulty on the next template, and allowed a
later request for the active difficulty to cancel a pending change. The lifecycle
case also verified that authorization emits no same-template work and that an old
job is still checked against its original immutable target after the new difficulty
becomes active.

The final synthetic GBT measurements were:

| Transactions | JSON | Raw transactions | Parse | Fingerprint | Job build | Managed allocation |
|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 3.24 MiB | 0.86 MiB | 86.9 ms | 3.9 ms | 12.7 ms | 3.3 MiB |
| 20,000 | 6.49 MiB | 1.72 MiB | 125.7 ms | 0.4 ms | 18.1 ms | 3.3 MiB |

The current real-Core large-mempool case took 494.7 seconds to seed 10,001
independent transactions and 1.9 seconds for published Stratum work, mining,
queue submission, and confirmation. The full command took 512.8 seconds.

The boundary test exposed a real startup race: Core briefly returned a cached
empty GBT after the mempool was seeded, which could leave the gateway's startup
job empty until the longpoll mempool timeout. Production now performs one bounded
startup catch-up GBT after Core's cache window. The test waits on authoritative
`/api/stats` height and transaction count before connecting miners, so it fails
instead of consuming or accepting an empty public job.

The separate `direct`, `mempool`, and `stratum` entry points were not rerun
because `all` covers those paths. The composite `stress` entry point was not
rerun because the suite runs `synthetic-gbt` and `large-mempool` directly.

## Configuration safety

The checked-in `config.json` is intentionally a template, not deployable secrets.
Both Stratum and API default to loopback, including when `listen_addr` is omitted.
Configuration loading rejects any remaining `REPLACE_` RPC credential or payout
address before contacting Core or opening listeners. Operators must provide real
RPC credentials and a network-matching payout address, and must explicitly widen
the Stratum bind plus firewall it for remote miners.

## Reproduction

Run the nonredundant suite from the repository root with the validated executable:

```powershell
$env:BITCOIND = 'C:\Program Files\Bitcoin\daemon\bitcoind.exe'
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-regtest-validation.ps1 suite
```

The suite starts a local regtest node when needed and removes its temporary
datadir after each mode. Do not run real-Core modes concurrently unless each
process has distinct RPC, P2P, ZMQ, Stratum, and API ports.

`core-restart` requires the RPC port to be unused at mode start so the harness can
own the node. It refuses to stop a reused/external Core unless the process marker,
loopback RPC URL, and datadir-under-current-workdir checks all pass. Core shutdown
is graceful to preserve the chain fixture; the abnormal crash is applied to the
owned gateway only after the pending JSON candidate has been flushed to disk.

## Production-path coverage

| Production stage | Real-Core evidence | Remaining mainnet-only risk |
|---|---|---|
| Config and startup guard | Chain identity checked before fixture mutation; production `ChainGuard` requires `IBD=false` after bootstrap; published gateway repeats the guard | Mainnet node sync duration and operator credentials/firewall |
| Template acquisition | Real GBT/longpoll, ZMQ, P2P-fast, startup cached-GBT catch-up, and owned-Core reconnect | Mainnet peer topology, mempool policy, and propagation latency |
| Miner protocol | Published TCP Stratum subscribe/authorize/notify/submit, BIP310, stale/late work, 4x8 extranonce sizes, and four witness/lifecycle boundary crosses | Real ASIC firmware diversity and WAN behavior |
| Block construction | Empty, witness mempool, 10,001-tx published-gateway, and 64 extranonce-matrix blocks | Mainnet target cannot be mined in a deterministic test |
| Submission and recovery | Production `BlockSubmitQueue`, Core `submitblock`, active-tip/full-txid confirmation, real offline retry persistence, gateway crash, and startup recovery | Host storage durability, OS/process failure modes, and operational restart procedures |

Regtest executes the production process and protocol path but intentionally uses
regtest consensus parameters. Mainnet proof-of-work, network magic/ports,
accumulated chain work, real IBD, and public-network propagation are not
simulated; `encoding`/`safety` cover those policy constants and fail-closed
branches, and the manual go-live checklist is still required.

## Follow-up validation (2026-11-09)

The 12 configured validation modes were run on Windows with the installed
Bitcoin Core v31.0.0 executable at
`C:\Program Files\Bitcoin\daemon\bitcoind.exe`. The aggregate run reached
`core-restart`, where the harness timed out waiting for the P2P ready log. That
log is intentionally emitted only once per peer runtime. The harness now checks
the UUID-protected peer API for `state=connected` and at least two TCP
connections, so the restart case proves reconnection from runtime state. The
focused rerun passed, as did the separately run `extranonce` and
`large-mempool` modes; together with the modes completed before the aggregate
run stopped, all 12 modes passed.

The same investigation found that pending-block persistence reopened its temp
file read-only before calling `Flush(true)`. Windows rejects flushing a
read-only handle, and the old code swallowed that error. It now opens a
read-write handle and lets a flush failure fail persistence so the queued block
can be retried. The focused `core-restart` recovery path and `shutdown` persistence
checks passed after the fix.

The 10,001-transaction case seeded the local mempool in 960.4 seconds. Once
seeded, the published gateway exposed the full template over Stratum, submitted
the block, and confirmed every transaction in 3.9 seconds; pending and failed
block directories were empty afterward. This measures the tested gateway path
on this Windows host, not mainnet peer propagation or ASIC behavior.

## P2P announcement improvements (2026-11-10)

P2P sessions now request both compact-block v2 announcements and BIP130
`sendheaders`. After the handshake, a separate task announces the current
Core-confirmed tip, responds to `getheaders` requests, and checks for tip changes
every two seconds. Peers share a header cache; RPC fetches have a three-second
timeout and a five-second failure cooldown. The receive loop does not await
these queries. Complete outbound frames are serialized with a five-second write
timeout, preserving framing when tip notifications and pong replies overlap.

Incoming `cmpctblock`/`block` processing still starts after the first 80 payload
bytes; it does not wait for the remaining payload to drain. Header parsing and
PoW target comparison use stack buffers. Repeated announcements of the active
tip return before another validation/publication, and `inv` requests use bounded
five-second coalescing instead of suppressing a locator for the entire session.
All new-parent linkage, height, compact-target, PoW, time, version, and fast-empty
policy checks remain in place. EOF diagnostics include the command/read stage
and received/expected byte counts.

Release build and publish completed with zero warnings/errors. Offline
`p2p-admin`, `safety`, and `encoding` passed. The added peer tests held a tip RPC
open while two P2P sessions completed negotiation and replied to ping, confirmed
one shared tip lookup, checked concurrent tip/pong frame integrity, and rejected
an inconsistent RPC header before retrying successfully.

Real Bitcoin Core v31.0.0 at
`C:\Program Files\Bitcoin\daemon\bitcoind.exe` passed `p2p-fast`, `all`,
`lifecycle`, and `core-restart` using isolated regtest datadirs. `getpeerinfo`
confirmed that Core knew the gateway's advertised header height and had received
its high-bandwidth compact-block preference. Sixteen duplicate peer
announcements left the original fast job active. In this local run the
block-generation-RPC-to-P2P-job-notification interval was 6.09 ms; it is not a
mainnet propagation measurement or a before/after benchmark. The published
gateway confirmed Stratum-submitted witness transactions, recorded 16 late/1
stale/1 unknown lifecycle outcomes with zero clean broadcast timeouts, and
reconnected/recovered its pending block after the owned Core restart.

## Independent P2P address discovery (2026-11-10)

Production candidate discovery no longer calls `getpeerinfo` or
`getnodeaddresses`. `P2pAddressDiscovery` uses separate short-lived P2P sessions
with one `getaddr` request after handshake, negotiates `sendaddrv2` before
`verack` for protocol 70016+, and accepts either legacy `addr` or BIP155 `addrv2`
responses. It does not run inside the fast-block receive loop. Existing public
manager constructors, configuration, dashboard/API snapshots, settings,
manual-peer JSON, blacklist, and winner-history formats are retained.

Configured/manual/current peers and an additional bounded
`p2p-fast-addresses.json` cache bootstrap discovery. When those sources fail,
three independently selected Bitcoin DNS seeds provide bootstrap endpoints;
the endpoints must still supply candidates through P2P. Discovery limits
concurrent sockets to two, sessions to eight seconds, message entries to 1,000,
and retained candidates to 4,096 with a 30-day age limit. Query slots are
reserved for learned sources beyond the current mining peers. IPv4/IPv6 actual
ports, service bits, timestamps, framing, CompactSize bounds, checksums, and
unknown-network handling are covered by offline checks. Unsupported address
networks are skipped rather than interpreted as IP addresses.

Release build/publish completed with zero warnings/errors; `p2p-admin`, `safety`,
and `encoding` passed. Address tests exercised a legacy 70015 peer, a BIP155 peer,
a modern peer returning legacy addresses, wrong-network/checksum rejection,
cache restoration, blacklist filtering without cache loss, and DNS query
cooldown/private-address rejection. Existing fixed/manual peer, blacklist-clear,
UUID authentication, IPv6 toggle, tip notification, and reconnect tests passed.

The installed Bitcoin Core v31.0.0 passed real isolated regtest `p2p-fast`,
`all`, `lifecycle`, and `core-restart`. The address fixture used Core's
test-only `addpeeraddress` RPC to populate its isolated address manager, then
the new P2P client obtained six/seven public candidates across two runs via `getaddr` and fed them
into the automatic pool. `getpeerinfo` remains only in the harness to verify
Core's common-tip/compact-block acknowledgement; it supplies no production
candidates. Published Stratum witness blocks were confirmed on chain, the
16-miner lifecycle recorded zero clean broadcast timeouts, and P2P reconnect
plus pending-block restart recovery passed. These tests demonstrate protocol
compatibility and the exercised production paths, not mainnet candidate quality
or a guarantee of faster remote block propagation.
