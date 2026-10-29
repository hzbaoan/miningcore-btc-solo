using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using MiningcoreBtcSolo;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.P2p;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Stratum;
using MiningcoreBtcSolo.Submit;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;
using NBitcoin;

// Pre-mainnet validation harness (regtest):
// share validation → full block assembly → submitblock → active-chain confirmation
//
// Modes:
//   direct   – empty-block + mempool multi-tx library paths
//   stratum  — gateway + Stratum V1 (with mempool txs when possible)
//   mempool  — multi-tx library path only
//   vardiff  — offline deterministic VarDiff regression checks (no bitcoind required)
//   encoding — offline encoding/reorg/P2P-fast boundary checks (no bitcoind required)
//   shutdown — offline submit-queue shutdown persistence check (no bitcoind required)
//   safety   — offline review-fix regression checks (no bitcoind required)
//   synthetic-gbt — offline 10k/20k transaction GBT parse/build stress checks
//   large-mempool — 10,001 transactions through published Stratum + submit queue
//   stress   — synthetic-gbt + large-mempool
//   p2p-fast — real header + coinbase-only fast job + mined active-chain block
//   lifecycle — real bitcoind + multi-miner clean/late/stale burst checks
//   extranonce — en1 1-4 x en2 1-8 share validation + submitblock + Stratum submit
//   core-restart — owned Core restart/disconnect + longpoll/ZMQ/P2P + pending recovery
//   all      — empty + mempool + stratum (default)

var mode = args.FirstOrDefault(a => !a.StartsWith('-')) ?? "all";
if (!IsKnownMode(mode))
{
    Console.Error.WriteLine($"unknown validation mode: {mode}");
    Environment.ExitCode = 2;
    return;
}
if (mode == "vardiff")
{
    RunVarDiffChecks();
    return;
}
if (mode == "encoding")
{
    RunEncodingChecks();
    return;
}
if (mode == "shutdown")
{
    await RunSubmitQueueShutdownCheckAsync();
    await RunDelayedRetryShutdownCheckAsync();
    return;
}
if (mode == "safety")
{
    await RunSafetyRegressionChecksAsync();
    return;
}
if (mode is "synthetic-gbt" or "stress")
{
    await RunSyntheticGbtStressChecksAsync();
    if (mode == "synthetic-gbt")
        return;
}

var configuredWorkDir = Environment.GetEnvironmentVariable("REGTEST_WORK_DIR");
var ownsWorkDir = string.IsNullOrWhiteSpace(configuredWorkDir);
var workDir = ownsWorkDir
    ? Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-regtest-" + Guid.NewGuid().ToString("N"))
    : Path.GetFullPath(configuredWorkDir!);
var datadir = Env("REGTEST_DATADIR", Path.Combine(workDir, "bitcoind"));
var rpcUrl = Env("REGTEST_RPC_URL", "http://127.0.0.1:18443");
var rpcUser = Env("REGTEST_RPC_USER", "regtest");
var rpcPass = Env("REGTEST_RPC_PASSWORD", "regtestpass");
var stratumHost = Env("REGTEST_STRATUM_HOST", "127.0.0.1");
var stratumPort = int.Parse(Env("REGTEST_STRATUM_PORT", "3333"));
var apiPort = int.Parse(Env("REGTEST_API_PORT", "7152"));
var keepBitcoind = args.Any(a => a is "--keep-bitcoind" or "-k");

Console.WriteLine("=== miningcore-btc-solo regtest validation ===");
Console.WriteLine($"mode={mode} datadir={datadir}");

Process? bitcoind = null;
Process? gateway = null;
var failures = new List<string>();
var passed = new List<string>();

try
{
    EnsureBitcoind(ref bitcoind, datadir, rpcUrl, rpcUser, rpcPass);
    var rpc = new BitcoinRpcClient(rpcUrl, rpcUser, rpcPass);

    WaitForRpc(rpc, TimeSpan.FromSeconds(60));
    await VerifyHarnessChainIdentityAsync(rpc);
    var payout = await EnsureWalletAndPayoutAsync(rpc);
    Console.WriteLine($"payout address (regtest): {payout}");

    // Mature coinbase funds / advance chain so GBT is healthy
    await EnsureChainReadyAsync(rpc);
    await ChainGuard.VerifyAsync(
        new AppConfig { NetworkName = "regtest" }, rpc, CancellationToken.None);
    Console.WriteLine("PASS production ChainGuard (chain=regtest, IBD=false)");

    if (mode == "core-restart")
    {
        Console.WriteLine();
        Console.WriteLine("--- CORE-RESTART: owned Core disconnect + source reconnect + pending recovery ---");
        try
        {
            if (bitcoind == null)
            {
                throw new InvalidOperationException(
                    "core-restart refuses an external/reused RPC node; it requires a bitcoind process started by this harness");
            }
            await RunOwnedCoreRestartRecoveryAsync(
                bitcoind,
                process => bitcoind = process,
                rpc, payout, workDir, datadir,
                rpcUrl, rpcUser, rpcPass, stratumHost);
            passed.Add("owned Core reconnect and pending recovery");
            Console.WriteLine("PASS owned Core reconnect and pending recovery");
        }
        catch (Exception ex)
        {
            failures.Add($"core-restart: {ex.Message}");
            Console.Error.WriteLine($"FAIL core-restart: {ex}");
        }
    }

    if (mode == "p2p-fast")
    {
        Console.WriteLine();
        Console.WriteLine("--- P2P-FAST: real header -> coinbase-only job -> mine -> submitblock -> active chain ---");
        try
        {
            await RunRealP2pFastPolicyPathAsync(rpc, payout, workDir);
            passed.Add("p2p-fast coinbase-only block");
            Console.WriteLine("PASS P2P-fast coinbase-only block");
        }
        catch (Exception ex)
        {
            failures.Add($"p2p-fast: {ex.Message}");
            Console.Error.WriteLine($"FAIL P2P-fast: {ex}");
        }
    }

    if (mode is "large-mempool" or "stress")
    {
        Console.WriteLine();
        Console.WriteLine("--- LARGE MEMPOOL: 10,001 tx -> published Stratum -> submit queue -> active chain ---");
        try
        {
            await RunLargeMempoolPathAsync(
                rpc, payout, workDir, rpcUrl, rpcUser, rpcPass, stratumHost,
                transactionCount: 10_001);
            passed.Add("large mempool 10001 tx end-to-end");
            Console.WriteLine("PASS large mempool 10,001 tx end-to-end");
        }
        catch (Exception ex)
        {
            failures.Add($"large-mempool: {ex.Message}");
            Console.Error.WriteLine($"FAIL large mempool: {ex}");
        }
    }

    if (mode is "direct" or "all")
    {
        Console.WriteLine();
        Console.WriteLine("--- [1/3] DIRECT empty: GBT → mine → ShareValidator → submitblock → getblock ---");
        try
        {
            await RunDirectPathAsync(rpc, payout, requireTxCount: 0, label: "empty");
            passed.Add("direct empty share→submitblock→chain");
            Console.WriteLine("PASS direct empty");
        }
        catch (Exception ex)
        {
            failures.Add($"direct-empty: {ex.Message}");
            Console.Error.WriteLine($"FAIL direct empty: {ex}");
        }
    }

    if (mode is "direct" or "mempool" or "all")
    {
        Console.WriteLine();
        Console.WriteLine("--- [2/3] DIRECT mempool: seed txs → GBT(txs>0) → mine full block → submitblock → verify txids ---");
        try
        {
            var mempoolTxids = await SeedMempoolAsync(rpc, count: 3);
            await RunDirectPathAsync(rpc, payout, requireTxCount: 1, label: "mempool", expectedTxids: mempoolTxids);
            passed.Add("direct mempool multi-tx share→submitblock→chain");
            Console.WriteLine("PASS direct mempool multi-tx");
        }
        catch (Exception ex)
        {
            failures.Add($"direct-mempool: {ex.Message}");
            Console.Error.WriteLine($"FAIL direct mempool: {ex}");
        }
    }

    if (mode == "core-restart")
    {
        Console.WriteLine("=== owned Core restart evidence ===");
        Console.WriteLine(
            $"  [{(passed.Any(p => p.Contains("owned Core", StringComparison.Ordinal)) ? "x" : " ")}] " +
            "readyz degradation + longpoll/ZMQ/P2P reconnect + pending-block startup recovery");
    }
    else if (mode == "extranonce")
    {
        Console.WriteLine();
        Console.WriteLine("--- EXTRANONCE: en1 1-4 x en2 1-8 share + submitblock + Stratum ---");
        try
        {
            await RunExtranonceMatrixAsync(
                rpc, payout, workDir, rpcUrl, rpcUser, rpcPass, stratumHost);
            passed.Add("extranonce matrix share→submitblock→chain");
            Console.WriteLine("PASS extranonce 4x8 share+block matrix");
        }
        catch (Exception ex)
        {
            failures.Add($"extranonce: {ex.Message}");
            Console.Error.WriteLine($"FAIL extranonce: {ex}");
        }
    }

    if (mode is "stratum" or "all" or "lifecycle")
    {
        Console.WriteLine();
        Console.WriteLine(mode == "lifecycle"
            ? "--- LIFECYCLE: 16 miners + 3-block burst + clean/late/stale checks ---"
            : "--- [3/3] STRATUM mempool: seed txs → gateway mining.submit → chain tip + txids ---");
        try
        {
            var mempoolTxids = mode == "lifecycle"
                ? new List<string>()
                : await SeedMempoolAsync(rpc, count: 3);
            if (mode == "lifecycle")
            {
                await RunRealP2pFastPolicyPathAsync(rpc, payout, workDir);
                passed.Add("p2p-fast coinbase-only block");
            }
            var configPath = await WriteRuntimeConfigAsync(
                workDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort,
                lifecycleMode: mode == "lifecycle");
            gateway = StartGateway(configPath);
            await WaitHttpAsync($"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));
            if (mode == "lifecycle")
            {
                await RunStratumLifecyclePathAsync(
                    rpc, stratumHost, stratumPort, apiPort, payout, minerCount: 16);
                passed.Add("stratum clean lifecycle burst");
                Console.WriteLine("PASS stratum clean lifecycle burst");
            }
            else
            {
                await RunStratumPathAsync(rpc, stratumHost, stratumPort, apiPort, expectedTxids: mempoolTxids);
                passed.Add("stratum mempool multi-tx share→submitblock→chain");
                Console.WriteLine("PASS stratum mempool multi-tx");
            }
        }
        catch (Exception ex)
        {
            failures.Add($"stratum: {ex.Message}");
            Console.Error.WriteLine($"FAIL stratum: {ex}");
        }
    }

    Console.WriteLine();
    if (mode == "extranonce")
    {
        Console.WriteLine("=== extranonce matrix evidence ===");
        Console.WriteLine(
            $"  [{(passed.Any(p => p.Contains("extranonce", StringComparison.Ordinal)) ? "x" : " ")}] " +
            "extranonce1_size 1-4 x extranonce2_size 1-8 share validation + submitblock + Stratum");
    }
    else if (mode is "large-mempool" or "stress")
    {
        Console.WriteLine("=== large-mempool regtest evidence ===");
        Console.WriteLine(
            $"  [{(passed.Any(p => p.Contains("large mempool", StringComparison.Ordinal)) ? "x" : " ")}] " +
            "10,001 mempool tx -> published Stratum -> submit queue -> active chain");
    }
    else
    {
        Console.WriteLine("=== pre-mainnet checklist (regtest evidence) ===");
        PrintChecklist(passed, failures);
    }
}
finally
{
    TryKill(gateway);
    if (!keepBitcoind)
        TryKill(bitcoind);
    else
        Console.WriteLine("leaving bitcoind running (--keep-bitcoind)");
    if (ownsWorkDir && (bitcoind == null || !keepBitcoind))
    {
        try { Directory.Delete(workDir, recursive: true); } catch { }
    }
}

if (failures.Count > 0)
{
    Console.Error.WriteLine($"FAILED ({failures.Count}): {string.Join("; ", failures)}");
    Environment.Exit(1);
}

Console.WriteLine("ALL REGTEST CHECKS PASSED - safe to proceed with mainnet config review.");
return;

// ---------------------------------------------------------------------------

static bool IsKnownMode(string mode) => mode is
    "all" or "direct" or "mempool" or "stratum" or "vardiff" or "encoding" or
    "shutdown" or "safety" or "synthetic-gbt" or "large-mempool" or "stress" or
    "p2p-fast" or "lifecycle" or "extranonce" or "core-restart";

static void RunHarnessModeChecks()
{
    if (!IsKnownMode("all") || !IsKnownMode("safety") || IsKnownMode("unknown"))
        throw new InvalidOperationException("regtest harness mode validation changed");
}

static void RunBlockSubsidyBoundaryChecks()
{
    if (BitcoinEncoding.BlockSubsidySat(209_999) != 5_000_000_000L ||
        BitcoinEncoding.BlockSubsidySat(210_000) != 2_500_000_000L ||
        BitcoinEncoding.BlockSubsidySat(839_999) != 625_000_000L ||
        BitcoinEncoding.BlockSubsidySat(840_000) != 312_500_000L ||
        BitcoinEncoding.BlockSubsidySat(210_000u * 64u) != 0)
        throw new InvalidOperationException("block subsidy halving boundaries changed");
}

static void RunP2pFastGuardChecks()
{
    if (!TemplateEngine.AllowsP2pFastEmpty("mainnet", 900_001, 0) ||
        !TemplateEngine.AllowsP2pFastEmpty("bitcoin", 900_001, 0) ||
        TemplateEngine.AllowsP2pFastEmpty("mainnet", 2016, 0) ||
        TemplateEngine.AllowsP2pFastEmpty("mainnet", 900_001, 1) ||
        TemplateEngine.AllowsP2pFastEmpty("regtest", 900_001, 0))
        throw new InvalidOperationException("P2P-fast policy guards changed");
}

static void RunP2pFastLogFormatCheck()
{
    var actual = SoloLog.FormatTemplateEvent(
        "new block",
        TemplateSource.P2pFast,
        "61",
        973_632,
        0,
        312_500_000,
        0x17020093,
        cleanJobs: true,
        peer: "127.0.0.1:8333",
        hash: "000000000000000000007d5abab3b2c016e7511454ec2aced3c96d7a3057efc7");
    const string expected =
        "new block source=p2p_fast job_id=61 height=973632 txs=0 reward=312500000 sat " +
        "nbits=17020093 clean_jobs=true peer=127.0.0.1:8333 " +
        "hash=000000000000000000007d5abab3b2c016e7511454ec2aced3c96d7a3057efc7";
    if (!string.Equals(actual, expected, StringComparison.Ordinal))
        throw new InvalidOperationException($"P2P-fast log format changed: {actual}");
}

static void RunP2pPeerConfigurationChecks()
{
    var cfg = new BitcoindConfig
    {
        P2pFastPeer = " 127.0.0.1:8333 ",
        P2pFastPeers = new List<string>
        {
            "127.0.0.1:8333",
            "10.0.0.12:8333",
            "",
            " 10.0.0.12:8333 "
        }
    };
    var peers = cfg.EffectiveP2pFastPeers;
    if (peers.Count != 2 || peers[0] != "127.0.0.1:8333" || peers[1] != "10.0.0.12:8333")
        throw new InvalidOperationException("P2P peer list did not merge, trim, and deduplicate legacy entries");

    var legacyOnly = new BitcoindConfig { P2pFastPeer = "node:8333" };
    if (legacyOnly.EffectiveP2pFastPeers.Count != 1 || legacyOnly.EffectiveP2pFastPeers[0] != "node:8333")
        throw new InvalidOperationException("legacy single P2P peer configuration is not preserved");

    var nullList = new BitcoindConfig { P2pFastPeers = null! };
    if (nullList.EffectiveP2pFastPeers.Count != 0)
        throw new InvalidOperationException("null P2P peer list was not treated as empty");
    var versionPayload = P2pFastPeer.BuildVersionPayload(
        new IPEndPoint(IPAddress.Loopback, 8333), startHeight: 900_000);
    if (versionPayload[^1] != 0)
        throw new InvalidOperationException("P2P version payload requested transaction relay");
    var compactBlockPayload = P2pFastPeer.BuildSendCmpct(2);
    if (compactBlockPayload.Length != 9 || compactBlockPayload[0] != 1 ||
        BinaryPrimitives.ReadUInt64LittleEndian(compactBlockPayload.AsSpan(1)) != 2)
        throw new InvalidOperationException("P2P fast peer did not request compact-block v2 announcements");
}

static void RunChainGuardSyncChecks()
{
    ChainGuard.EnsureMiningReady(initialBlockDownload: false, blocks: 900_000, headers: 900_000);
    ChainGuard.EnsureMiningReady(initialBlockDownload: false, blocks: 899_999, headers: 900_000);
    ExpectException<InvalidOperationException>(
        () => ChainGuard.EnsureMiningReady(
            initialBlockDownload: true, blocks: 850_000, headers: 900_000),
        "initial block download",
        "IBD startup");
    ExpectException<InvalidOperationException>(
        () => ChainGuard.EnsureMiningReady(
            initialBlockDownload: false, blocks: -1, headers: 900_000),
        "invalid blocks/headers",
        "malformed chain counters");
}

static void RunBitcoinProtocolNegativeChecks()
{
    var secureDefaults = new AppConfig();
    if (secureDefaults.Stratum.ListenAddr != "127.0.0.1" ||
        secureDefaults.Api.ListenAddr != "127.0.0.1")
        throw new InvalidOperationException("network listeners no longer default to loopback");

    ExpectException<InvalidOperationException>(
        () => new AppConfig { NetworkName = "signet" }.Validate(),
        "Unsupported network: signet",
        "signet configuration");

    var placeholderConfig = OfflineConfig(Path.GetTempPath());
    placeholderConfig.Bitcoind.RpcPassword = "REPLACE_WITH_YOUR_RPC_PASSWORD";
    ExpectException<InvalidOperationException>(
        placeholderConfig.Validate,
        "placeholder values",
        "placeholder production configuration");

    ExpectException<FormatException>(
        () => BitcoinEncoding.TargetHexToLe(new string('0', 62)),
        "exactly 32 bytes",
        "31-byte GBT target");
    ExpectException<FormatException>(
        () => BitcoinEncoding.TargetHexToLe(new string('0', 66)),
        "exactly 32 bytes",
        "33-byte GBT target");
    ExpectException<FormatException>(
        () => BitcoinEncoding.TargetHexToLe(new string('0', 63) + "x"),
        "exactly 32 bytes",
        "malformed 32-byte GBT target");
    ExpectException<FormatException>(
        () => BitcoinEncoding.TargetHexToLe(" " + new string('0', 63)),
        "exactly 32 bytes",
        "whitespace-padded GBT target");

    const uint nbits = 0x207fffff;
    var targetHex = Hex.Encode(Hex.ReverseCopy(BitcoinEncoding.CompactTargetToLe(nbits)));
    var cfg = OfflineConfig(Path.GetTempPath());
    cfg.Coinbase.Message = "";
    var builder = new JobBuilder(cfg);

    GbtResponse Gbt() => new()
    {
        Version = 0x20000000,
        PreviousBlockhash = new string('1', 64),
        CoinbaseValue = 5_000_000_000,
        Target = targetHex,
        CurTime = 1_700_000_001,
        Bits = nbits.ToString("x8"),
        Height = 17,
        Transactions = Array.Empty<GbtTx>(),
        CoinbaseAux = new GbtCoinbaseAux { Flags = "01aa" },
        Mintime = 1_700_000_001
    };

    var valid = Gbt();
    var flagsJob = builder.FromGbt(valid, TemplateSource.Startup);
    ReadOnlySpan<byte> expectedScriptPrefix = [0x01, 0x11, 0x01, 0xaa];
    if (flagsJob.Coinbase1.Length < expectedScriptPrefix.Length ||
        !flagsJob.Coinbase1.AsSpan(flagsJob.Coinbase1.Length - expectedScriptPrefix.Length)
            .SequenceEqual(expectedScriptPrefix))
        throw new InvalidOperationException("coinbaseaux.flags was not appended byte-for-byte");

    var malformedPrevhash = Gbt();
    malformedPrevhash.PreviousBlockhash = new string('1', 63);
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(malformedPrevhash, TemplateSource.Startup),
        "previousblockhash",
        "short GBT previousblockhash");
    var prefixedPrevhash = Gbt();
    prefixedPrevhash.PreviousBlockhash = "0x" + new string('1', 62);
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(prefixedPrevhash, TemplateSource.Startup),
        "previousblockhash",
        "prefixed GBT previousblockhash");

    var negativeCoinbase = Gbt();
    negativeCoinbase.CoinbaseValue = -1;
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(negativeCoinbase, TemplateSource.Startup),
        "money range",
        "negative GBT coinbasevalue");
    var excessiveCoinbase = Gbt();
    excessiveCoinbase.CoinbaseValue = 2_100_000_000_000_001L;
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(excessiveCoinbase, TemplateSource.Startup),
        "money range",
        "GBT coinbasevalue above MAX_MONEY");

    const string validWitnessCommitment =
        "6a24aa21a9ed0000000000000000000000000000000000000000000000000000000000000000";
    var validWitness = Gbt();
    validWitness.DefaultWitnessCommitment = validWitnessCommitment;
    if (!builder.FromGbt(validWitness, TemplateSource.Startup).HasWitnessCommitment)
        throw new InvalidOperationException("valid BIP141 witness commitment was rejected");
    var validWitnessWithTrailingData = Gbt();
    validWitnessWithTrailingData.DefaultWitnessCommitment = validWitnessCommitment + "00";
    if (!builder.FromGbt(validWitnessWithTrailingData, TemplateSource.Startup).HasWitnessCommitment)
        throw new InvalidOperationException("valid BIP141 witness commitment trailing data was rejected");

    foreach (var malformedCommitment in new[]
             {
                 "6a24aa21a9ed" + new string('0', 62),
                 "6a24aa21a9ec" + new string('0', 64),
                 validWitnessCommitment + "0"
             })
    {
        var invalidWitness = Gbt();
        invalidWitness.DefaultWitnessCommitment = malformedCommitment;
        ExpectException<InvalidOperationException>(
            () => builder.FromGbt(invalidWitness, TemplateSource.Startup),
            "BIP141 commitment",
            "malformed default_witness_commitment");
    }

    var invertedTimes = Gbt();
    invertedTimes.Mintime = invertedTimes.CurTime + 1;
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(invertedTimes, TemplateSource.Startup),
        "earlier than mintime",
        "GBT curtime before mintime");

    var missingRequiredVersionBits = Gbt();
    missingRequiredVersionBits.Vbrequired = 0x00001000;
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(missingRequiredVersionBits, TemplateSource.Startup),
        "missing vbrequired bits",
        "GBT version missing vbrequired bits");

    var mismatchedTarget = Gbt();
    mismatchedTarget.Target = new string('0', 64);
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(mismatchedTarget, TemplateSource.Startup),
        "does not exactly match compact bits",
        "GBT target/bits mismatch");

    var invalidCompact = Gbt();
    invalidCompact.Bits = "1d80ffff";
    invalidCompact.Target = new string('0', 64);
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(invalidCompact, TemplateSource.Startup),
        "invalid compact target",
        "negative compact target");

    var malformedBits = Gbt();
    malformedBits.Bits = " 07fffff";
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(malformedBits, TemplateSource.Startup),
        "exactly 4 bytes of hex",
        "malformed compact bits");

    var zeroCompact = Gbt();
    zeroCompact.Bits = "01000001";
    zeroCompact.Target = new string('0', 64);
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(zeroCompact, TemplateSource.Startup),
        "invalid compact target",
        "zero compact target after right shift");

    var overPowLimit = Gbt();
    overPowLimit.Bits = "2100ffff";
    overPowLimit.Target = Hex.Encode(Hex.ReverseCopy(
        BitcoinEncoding.CompactTargetToLe(0x2100ffff)));
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(overPowLimit, TemplateSource.Startup),
        "network proof-of-work limit",
        "compact target over regtest pow limit");

    var missingTxid = Gbt();
    missingTxid.Transactions =
    [
        new GbtTx
        {
            Data = [0x01],
            TxId = null,
            Hash = new string('2', 64)
        }
    ];
    ExpectException<InvalidOperationException>(
        () => builder.ComputeTemplateKeyParts(missingTxid),
        "missing txid",
        "wtxid fallback");

    var packedMissingTxidJson =
        $"{{\"transactions\":[{{\"data\":\"01\",\"hash\":\"{new string('2', 64)}\"}}]}}";
    ExpectException<JsonException>(
        () => JsonSerializer.Deserialize<GbtResponse>(packedMissingTxidJson),
        "missing txid",
        "packed GBT wtxid fallback");

    var invalidFlags = Gbt();
    invalidFlags.CoinbaseAux = new GbtCoinbaseAux { Flags = "abc" };
    ExpectException<InvalidOperationException>(
        () => builder.FromGbt(invalidFlags, TemplateSource.Startup),
        "even-length hex",
        "malformed coinbaseaux.flags");

    var emptyFingerprint = JobBuilder.TxSetFingerprint(ReadOnlySpan<byte>.Empty);
    if (emptyFingerprint.Length != 32)
        throw new InvalidOperationException("empty transaction-set fingerprint is shorter than 128 bits");

    var singleHeaderPayload = new byte[82];
    singleHeaderPayload[0] = 1;
    BinaryPrimitives.WriteUInt32LittleEndian(singleHeaderPayload.AsSpan(1, 4), 0x20000000);
    singleHeaderPayload[^1] = 0;
    if (!P2pFastPeer.TryParseHeadersPayload(singleHeaderPayload, out var parsed) || parsed.Length != 1)
        throw new InvalidOperationException("valid one-header P2P payload was rejected");

    const int maxHeaders = 2000;
    var maxHeadersPayload = new byte[3 + maxHeaders * 81];
    maxHeadersPayload[0] = 0xfd;
    BinaryPrimitives.WriteUInt16LittleEndian(maxHeadersPayload.AsSpan(1, 2), maxHeaders);
    Span<byte> previousHeaderHashLe = stackalloc byte[32];
    var headerOffset = 3;
    for (var i = 0; i < maxHeaders; i++)
    {
        var header = maxHeadersPayload.AsSpan(headerOffset, 80);
        BinaryPrimitives.WriteUInt32LittleEndian(header[..4], 0x20000000);
        previousHeaderHashLe.CopyTo(header.Slice(4, 32));
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(68, 4), (uint)i + 1);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(72, 4), 0x207fffff);
        BitcoinEncoding.DoubleSha256(header, previousHeaderHashLe);
        maxHeadersPayload[headerOffset + 80] = 0;
        headerOffset += 81;
    }
    if (!P2pFastPeer.TryParseHeadersPayload(maxHeadersPayload, out parsed) ||
        parsed.Length != maxHeaders)
        throw new InvalidOperationException("P2P headers count boundary 2000 was rejected");
    if (P2pFastPeer.TryParseHeadersPayload([0xfd, 0xd1, 0x07], out _))
        throw new InvalidOperationException("P2P headers count above 2000 was accepted");
    if (P2pFastPeer.TryParseHeadersPayload([0xfd, 0x01, 0x00], out _))
        throw new InvalidOperationException("non-canonical P2P headers CompactSize was accepted");

    const uint parentHeight = 900_000;
    const uint parentMtp = 1_700_000_000;
    const long now = 1_700_000_100;
    var powTarget = BitcoinEncoding.CompactTargetToLe(0x1d00ffff);
    var tip = new ChainTip
    {
        HashHex = new string('1', 64),
        Height = parentHeight,
        MedianTimePast = parentMtp,
        Nbits = 0x1d00ffff,
        Version = 0x20000000,
        Vbrequired = 0,
        TargetLe = powTarget,
        NetworkDifficulty = 1
    };

    string? ValidateParent(
        uint blockTime = parentMtp + 1,
        uint blockVersion = 0x20000000,
        string? blockHash = null,
        ChainTip? parent = null,
        string? prevhash = null,
        uint? blockHeight = null,
        uint? blockNbits = null) =>
        TemplateEngine.ValidateP2pFastParentHeader(
            "mainnet",
            parent ?? tip,
            prevhash ?? (parent ?? tip).HashHex,
            blockHash ?? new string('0', 64),
            blockTime,
            blockHeight ?? (parent ?? tip).Height + 1,
            blockNbits ?? (parent ?? tip).Nbits,
            blockVersion,
            now);

    if (ValidateParent() != null)
        throw new InvalidOperationException("valid P2P parent header guard fixture was rejected");
    if (ValidateParent(prevhash: new string('2', 64)) != "prevhash_mismatch")
        throw new InvalidOperationException("unlinked P2P parent header was accepted");
    if (ValidateParent(blockHeight: parentHeight + 2) != "height_mismatch")
        throw new InvalidOperationException("P2P parent with the wrong height was accepted");
    if (ValidateParent(blockNbits: tip.Nbits + 1) != "nbits_mismatch")
        throw new InvalidOperationException("P2P parent with the wrong nBits was accepted");
    if (ValidateParent(blockTime: parentMtp) != "time_too_old")
        throw new InvalidOperationException("P2P parent at MTP was accepted");
    if (ValidateParent(blockTime: (uint)(now + 2 * 60 * 60 + 1)) != "time_too_new")
        throw new InvalidOperationException("P2P parent over the future-time limit was accepted");
    if (ValidateParent(blockVersion: 3) != "obsolete_version")
        throw new InvalidOperationException("obsolete P2P parent version was accepted");
    if (ValidateParent(blockHash: new string('f', 64)) != "invalid_pow")
        throw new InvalidOperationException("P2P parent with invalid proof of work was accepted");
    if (TemplateEngine.ConservativeMtpUpperBound(parentMtp, parentMtp + 1) != parentMtp + 1 ||
        TemplateEngine.ConservativeMtpUpperBound(parentMtp + 1, parentMtp) != parentMtp + 1)
        throw new InvalidOperationException("P2P child MTP guard stopped using the conservative upper bound");

    var requiredVersionTip = new ChainTip
    {
        HashHex = tip.HashHex,
        Height = tip.Height,
        MedianTimePast = tip.MedianTimePast,
        Nbits = tip.Nbits,
        Version = tip.Version,
        Vbrequired = 0x00001000,
        TargetLe = tip.TargetLe,
        NetworkDifficulty = tip.NetworkDifficulty
    };
    if (ValidateParent(parent: requiredVersionTip) != "missing_required_version_bits")
        throw new InvalidOperationException("P2P parent missing vbrequired bits was accepted");

    Console.WriteLine("PASS Bitcoin protocol negative checks");
}

static void ExpectException<TException>(
    Action action,
    string expectedMessageFragment,
    string label)
    where TException : Exception
{
    try
    {
        action();
    }
    catch (TException ex) when (ex.Message.Contains(expectedMessageFragment, StringComparison.Ordinal))
    {
        return;
    }

    throw new InvalidOperationException($"{label} did not fail closed with the expected error");
}

static void RunVarDiffChecks()
{
    var config = new DifficultyConfig
    {
        Min = 1,
        Max = 1_000_000,
        TargetTimeSecs = 5,
        RetargetTimeSecs = 30,
        RetargetShareBurst = 8,
        VariancePercent = 30,
        RetargetSmoothing = 0.25,
        MaxStepUp = 2,
        MaxStepUpBurst = 32,
        MaxStepDown = 0.5
    };

    var beforeWindow = VarDiffCalculator.Evaluate(config, 1024, 0, 0, 29.9, false);
    AssertVarDiff(!beforeWindow.ResetWindow, "zero-share window fired early");

    var silence = VarDiffCalculator.Evaluate(config, 1024, 0, 0, 30, false);
    AssertVarDiff(silence.ResetWindow && silence.ApplyDifficulty, "zero-share window did not retarget");
    AssertNear(silence.NextDifficulty, 512, "zero-share step-down");

    var luckyPair = VarDiffCalculator.Evaluate(config, 1, 2, 2, 0.1, true);
    AssertVarDiff(!luckyPair.ResetWindow, "two-share sample bypassed retarget_share_burst");

    var realBurst = VarDiffCalculator.Evaluate(config, 1, 8, 8, 0.8, true);
    AssertVarDiff(realBurst.ApplyDifficulty && realBurst.BurstUp, "configured burst did not raise difficulty");
    AssertNear(realBurst.NextDifficulty, 32, "burst max-step cap");

    // Eight shares from old assigned work represent eight units. With current=32,
    // weighted estimation moves toward 50, not current*50 (the old compounding bug).
    var oldWorkShares = VarDiffCalculator.Evaluate(config, 32, 8, 8, 0.8, true);
    AssertVarDiff(oldWorkShares.ApplyDifficulty && oldWorkShares.BurstUp,
        "weighted old-work samples did not retarget");
    AssertNear(oldWorkShares.NextDifficulty, 50, "old-work weighted estimate");

    // With six expected shares/window, five and eight shares are normal variance.
    var stableLow = VarDiffCalculator.Evaluate(config, 100, 5, 500, 30, false);
    AssertVarDiff(stableLow.ResetWindow && !stableLow.ApplyDifficulty,
        "stable five-share window changed difficulty");
    var stableHigh = VarDiffCalculator.Evaluate(config, 100, 8, 800, 30, false);
    AssertVarDiff(stableHigh.ResetWindow && !stableHigh.ApplyDifficulty,
        "stable eight-share window changed difficulty");
    var stableEarlyHigh = VarDiffCalculator.Evaluate(config, 100, 8, 800, 29, true);
    AssertVarDiff(!stableEarlyHigh.ResetWindow && !stableEarlyHigh.ApplyDifficulty,
        "stable eighth share incorrectly triggered the burst path");

    var stableRatio = 1.0;
    var stableShares = new[] { 5, 7, 4, 8, 6, 5 };
    for (var windowIndex = 0; windowIndex < 24; windowIndex++)
    {
        var shares = stableShares[windowIndex % stableShares.Length];
        var stable = VarDiffCalculator.Evaluate(
            config, 100, shares, shares * 100, 30, false, stableRatio);
        AssertVarDiff(stable.ResetWindow && !stable.ApplyDifficulty,
            $"stable multi-window sequence retargeted at window {windowIndex}");
        stableRatio = stable.SmoothedRatio;
    }

    // A single noisy window updates the EWMA; persistence confirms a hashrate change.
    var sparseFirst = VarDiffCalculator.Evaluate(config, 100, 1, 100, 30, false);
    AssertVarDiff(sparseFirst.ResetWindow && !sparseFirst.ApplyDifficulty,
        "single sparse window changed difficulty");
    var sparseSecond = VarDiffCalculator.Evaluate(
        config, 100, 1, 100, 30, false, sparseFirst.SmoothedRatio);
    AssertVarDiff(sparseSecond.ResetWindow && sparseSecond.ApplyDifficulty,
        "persistent sparse windows did not lower difficulty");
    AssertNear(sparseSecond.NextDifficulty, 63.54166666666667,
        "smoothed sparse-window step-down");

    var fastFirst = VarDiffCalculator.Evaluate(config, 100, 12, 1200, 30, false);
    AssertVarDiff(fastFirst.ResetWindow && !fastFirst.ApplyDifficulty,
        "single fast window changed difficulty");
    var fastSecond = VarDiffCalculator.Evaluate(
        config, 100, 12, 1200, 30, false, fastFirst.SmoothedRatio);
    AssertVarDiff(fastSecond.ResetWindow && fastSecond.ApplyDifficulty,
        "persistent fast windows did not raise difficulty");
    AssertNear(fastSecond.NextDifficulty, 143.75, "smoothed fast-window step-up");

    var atMinimum = VarDiffCalculator.Evaluate(config, 1, 0, 0, 30, false);
    AssertVarDiff(atMinimum.ResetWindow && !atMinimum.ApplyDifficulty,
        "minimum difficulty should consume the silent window without reapplying");

    Console.WriteLine("PASS vardiff offline checks");
}

static void AssertVarDiff(bool condition, string message)
{
    if (!condition)
        throw new InvalidOperationException(message);
}

static void AssertNear(double actual, double expected, string message)
{
    var tolerance = Math.Max(1e-9, Math.Abs(expected) * 1e-9);
    if (Math.Abs(actual - expected) > tolerance)
        throw new InvalidOperationException($"{message}: expected={expected:G17} actual={actual:G17}");
}

static void RunEncodingChecks()
{
    RunBitcoinProtocolNegativeChecks();

    var compactSmall = BitcoinEncoding.CompactTargetToLe(0x0300ffff);
    AssertBytes(compactSmall, new byte[] { 0xff, 0xff, 0x00, 0x00 }, 4,
        "compact target exponent=3 little-endian placement");

    var compactTiny = BitcoinEncoding.CompactTargetToLe(0x02008000);
    AssertBytes(compactTiny, new byte[] { 0x80, 0x00, 0x00 }, 3,
        "compact target exponent=2 little-endian placement");

    var compactExponent33 = BitcoinEncoding.CompactTargetToLe(0x210000ff);
    if (compactExponent33[30] != 0xff || compactExponent33.Where((_, i) => i != 30).Any(x => x != 0))
        throw new InvalidOperationException(
            $"compact target exponent=33 boundary mismatch: {Hex.Encode(compactExponent33)}");
    if (BitcoinEncoding.CompactTargetToLe(0x21010000).Any(x => x != 0))
        throw new InvalidOperationException("overflowing compact target exponent=33 was not rejected");
    var compactExponent34 = BitcoinEncoding.CompactTargetToLe(0x220000ff);
    if (compactExponent34[31] != 0xff || compactExponent34.Take(31).Any(x => x != 0))
        throw new InvalidOperationException(
            $"compact target exponent=34 boundary mismatch: {Hex.Encode(compactExponent34)}");

    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(0x007fffff),
        new byte[] { 0x03, 0xff, 0xff, 0x7f }, 4,
        "BIP34 height without sign byte");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(1),
        new byte[] { 0x51 }, 1,
        "BIP34 small integer OP_1 encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(16),
        new byte[] { 0x60 }, 1,
        "BIP34 small integer OP_16 encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(17),
        new byte[] { 0x01, 0x11 }, 2,
        "BIP34 height 17 data-push encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(0x00800000),
        new byte[] { 0x04, 0x00, 0x00, 0x80, 0x00 }, 5,
        "BIP34 height with sign byte");

    var current = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = 900_001,
        PrevhashBe = new string('a', 64)
    };
    if (TemplateEngine.ClassifyGbtTip(current, new string('b', 64), current.Height) != GbtTipRelation.Reorg)
        throw new InvalidOperationException("same-height different-prev template was not classified as a reorg");
    if (TemplateEngine.ClassifyGbtTip(current, current.PrevhashBe, current.Height) != GbtTipRelation.SameTip)
        throw new InvalidOperationException("same-height same-prev template was not classified as SameTip");
    if (TemplateEngine.ClassifyGbtTip(current, current.PrevhashBe, current.Height - 1) != GbtTipRelation.Behind)
        throw new InvalidOperationException("lower-height template was not classified as Behind");

    // P2P announces block B at H and publishes a speculative job mining H+1 on B.
    // A Core GBT still mining H on A is stale relative to that job, not a reorg.
    var fast = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.P2pFast,
        Height = current.Height + 1,
        PrevhashBe = new string('b', 64)
    };
    if (TemplateEngine.ClassifyGbtTip(fast, current.PrevhashBe, current.Height) != GbtTipRelation.Behind)
        throw new InvalidOperationException("pre-fast Core GBT was not classified as Behind");

    // Core confirms B: the full H+1 template replaces the empty fast template
    // without a clean-chain switch.
    if (TemplateEngine.ClassifyGbtTip(fast, fast.PrevhashBe, fast.Height) != GbtTipRelation.SameTip)
        throw new InvalidOperationException("Core confirmation of the P2P-fast tip was not classified as SameTip");

    // Core confirms competing block C at H: its H+1 GBT is authoritative reorg.
    if (TemplateEngine.ClassifyGbtTip(fast, new string('c', 64), fast.Height) != GbtTipRelation.Reorg)
        throw new InvalidOperationException("competing Core tip after P2P-fast was not classified as Reorg");

    var confirmingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = fast.Height,
        PrevhashBe = fast.PrevhashBe,
        SubmitOld = false
    };
    if (TemplateEngine.ShouldCleanGbtUpdate(fast, confirmingGbt))
        throw new InvalidOperationException("confirming GBT incorrectly invalidated same-tip P2P-fast work");

    var precedingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = confirmingGbt.Height,
        PrevhashBe = confirmingGbt.PrevhashBe
    };
    if (!TemplateEngine.ShouldCleanGbtUpdate(precedingGbt, confirmingGbt))
        throw new InvalidOperationException("submitold=false did not invalidate the preceding authoritative GBT");

    var competingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = fast.Height,
        PrevhashBe = new string('c', 64),
        SubmitOld = true
    };
    if (!TemplateEngine.ShouldCleanGbtUpdate(fast, competingGbt))
        throw new InvalidOperationException("competing GBT did not clean P2P-fast work");

    Console.WriteLine("PASS encoding/reorg/P2P-fast boundary checks");
}

static void AssertBytes(byte[] actual, byte[] expectedPrefix, int length, string message)
{
    if (actual.Length != 32 && message.StartsWith("compact", StringComparison.Ordinal))
        throw new InvalidOperationException($"{message}: target length={actual.Length}");
    if (actual.Length < length || !actual.AsSpan(0, length).SequenceEqual(expectedPrefix))
        throw new InvalidOperationException(
            $"{message}: expected prefix={Hex.Encode(expectedPrefix)} actual={Hex.Encode(actual.AsSpan(0, Math.Min(actual.Length, length)))}");
}

static (string BlockHex, string Hash) CreateQueueTestBlock(byte seed)
{
    var block = new byte[81];
    for (var i = 0; i < 80; i++)
        block[i] = (byte)(seed + i * 17);
    block[80] = 1;
    var hashLe = BitcoinEncoding.DoubleSha256(block.AsSpan(0, 80));
    return (Hex.Encode(block), Hex.Encode(Hex.ReverseCopy(hashLe)));
}

static async Task RunSubmitQueueShutdownCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-shutdown-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = new AppConfig
        {
            Runtime = new RuntimeConfig { DataDir = root }
        };
        using var rpc = new BitcoinRpcClient(
            "http://127.0.0.1:1", "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        await queue.StartAsync(CancellationToken.None);
        var candidate = CreateQueueTestBlock(1);
        await queue.EnqueueFoundBlockAsync(candidate.BlockHex, candidate.Hash, 900_001);
        await queue.StopAsync(TimeSpan.FromMilliseconds(50));

        var pending = Directory.GetFiles(queue.PendingDir, "*.json");
        if (pending.Length != 1)
            throw new InvalidOperationException($"graceful shutdown persisted {pending.Length} pending blocks, expected 1");
        if (!queue.IsStopped)
            throw new InvalidOperationException("submit queue stop task did not complete successfully");

        Console.WriteLine("PASS submit queue graceful-shutdown persistence check");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunDelayedRetryShutdownCheckAsync()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-delayed-shutdown-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    BlockSubmitQueue? queue = null;
    Task? serverTask = null;

    try
    {
        serverTask = Task.Run(async () =>
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("method").GetString() != "submitblock")
                throw new InvalidOperationException("delayed retry test received a non-submitblock request");

            var response = Encoding.UTF8.GetBytes(
                "{\"result\":\"prev-blk-not-found\",\"error\":null,\"id\":\"1\"}");
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();
        });

        var cfg = new AppConfig
        {
            Runtime = new RuntimeConfig { DataDir = root }
        };
        using var rpc = new BitcoinRpcClient(
            $"http://127.0.0.1:{port}/", "unused", "unused", requestTimeoutSecs: 15);
        queue = new BlockSubmitQueue(
            cfg, rpc, new MetricsStore(), attemptDelaysMs: [0], delayedRetryDelayMs: 60_000);
        await queue.StartAsync(CancellationToken.None);

        // Make the first durability attempt fail. The delayed-retry owner must retain
        // the binary candidate until StopAsync can retry after storage is restored.
        Directory.Delete(queue.PendingDir);
        File.WriteAllText(queue.PendingDir, "blocks pending-directory creation");

        var block = CreateQueueTestBlock(9);
        var binaryCandidate = new BlockCandidate(Hex.Decode(block.BlockHex));
        await queue.EnqueueFoundBlockAsync(binaryCandidate, block.Hash, 900_009);
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (queue.DelayedRetryState != (1, 1) && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        if (queue.DelayedRetryState != (1, 1))
        {
            throw new InvalidOperationException(
                $"delayed retry ownership was not established: {queue.DelayedRetryState}");
        }

        File.Delete(queue.PendingDir);
        Directory.CreateDirectory(queue.PendingDir);

        var stopTimer = Stopwatch.StartNew();
        await queue.StopAsync(TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(5));
        stopTimer.Stop();
        if (stopTimer.Elapsed >= TimeSpan.FromSeconds(5))
            throw new InvalidOperationException("shutdown waited for the full delayed-retry interval");
        if (queue.DelayedRetryState != (0, 0))
            throw new InvalidOperationException($"shutdown retained delayed retry state: {queue.DelayedRetryState}");

        var pendingFiles = Directory.GetFiles(queue.PendingDir, "*.json");
        if (pendingFiles.Length != 1)
            throw new InvalidOperationException(
                $"delayed retry shutdown persisted {pendingFiles.Length} blocks, expected 1");
        using var persisted = JsonDocument.Parse(await File.ReadAllBytesAsync(pendingFiles[0]));
        if (persisted.RootElement.GetProperty("hash").GetString() != block.Hash ||
            persisted.RootElement.GetProperty("blockHex").GetString() != block.BlockHex)
            throw new InvalidOperationException("shutdown changed the delayed binary candidate");

        Console.WriteLine("PASS delayed retry cancellation/task ownership shutdown check");
    }
    finally
    {
        listener.Stop();
        if (queue != null)
        {
            if (File.Exists(queue.PendingDir))
            {
                File.Delete(queue.PendingDir);
                Directory.CreateDirectory(queue.PendingDir);
            }
            try { await queue.StopAsync(TimeSpan.FromSeconds(2)); } catch { }
        }
        if (serverTask != null)
        {
            try { await serverTask; } catch { }
        }
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunPendingTempRecoveryCheckAsync()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-temp-recovery-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    BlockSubmitQueue? recoveredQueue = null;

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(
            cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var beforeCrash = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var tempCandidate = CreateQueueTestBlock(4);
        var finalCandidate = CreateQueueTestBlock(5);
        var tempHash = tempCandidate.Hash;
        var finalHash = finalCandidate.Hash;
        var corruptHash = new string('6', 64);

        ExpectException<ArgumentException>(
            () => beforeCrash.EnqueueFoundBlockAsync("zz", tempHash, 1).GetAwaiter().GetResult(),
            "malformed",
            "non-hex queued block");
        ExpectException<ArgumentException>(
            () => beforeCrash.EnqueueFoundBlockAsync(new string('0', 163), tempHash, 1)
                .GetAwaiter().GetResult(),
            "malformed",
            "odd-length queued block");
        ExpectException<ArgumentException>(
            () => beforeCrash.EnqueueFoundBlockAsync(
                tempCandidate.BlockHex, "..\\outside", 1).GetAwaiter().GetResult(),
            "malformed",
            "path-like queued block hash");
        ExpectException<ArgumentException>(
            () => beforeCrash.EnqueueFoundBlockAsync(
                tempCandidate.BlockHex, finalHash, 1).GetAwaiter().GetResult(),
            "does not match",
            "queued block/header hash mismatch");

        static void WritePending(string path, uint height, string hash, string blockHex)
        {
            var json = JsonSerializer.Serialize(new
            {
                height,
                hash,
                blockHex,
                createdAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            });
            File.WriteAllText(path, json);
        }

        var tempPath = Path.Combine(beforeCrash.PendingDir, tempHash + ".json.tmp");
        WritePending(tempPath, 900_004, tempHash, tempCandidate.BlockHex);
        var finalPath = Path.Combine(beforeCrash.PendingDir, finalHash + ".json");
        WritePending(finalPath, 900_005, finalHash, finalCandidate.BlockHex);
        WritePending(finalPath + ".tmp", 900_005, finalHash, "cc");
        var corruptPath = Path.Combine(beforeCrash.PendingDir, corruptHash + ".json.tmp");
        File.WriteAllText(corruptPath, "{not-json");
        var mismatchedPath = Path.Combine(beforeCrash.PendingDir, new string('7', 64) + ".json");
        WritePending(mismatchedPath, 900_006, tempHash, tempCandidate.BlockHex);
        var traversalPath = Path.Combine(beforeCrash.PendingDir, new string('8', 64) + ".json");
        WritePending(traversalPath, 900_007, "..\\outside", tempCandidate.BlockHex);

        var submittedBlocks = new List<string>();
        var serverTask = Task.Run(async () =>
        {
            for (var i = 0; i < 2; i++)
            {
                var context = await listener.GetContextAsync();
                using var request = await JsonDocument.ParseAsync(context.Request.InputStream);
                if (request.RootElement.GetProperty("method").GetString() != "submitblock")
                    throw new InvalidOperationException("temp recovery issued a non-submitblock request");
                submittedBlocks.Add(request.RootElement.GetProperty("params")[0].GetString()!);
                var response = Encoding.UTF8.GetBytes(
                    $"{{\"result\":null,\"error\":null,\"id\":\"{i + 1}\"}}");
                context.Response.ContentType = "application/json";
                context.Response.ContentLength64 = response.Length;
                await context.Response.OutputStream.WriteAsync(response);
                context.Response.Close();
            }
        });

        var metrics = new MetricsStore();
        recoveredQueue = new BlockSubmitQueue(cfg, rpc, metrics);
        await recoveredQueue.StartAsync(CancellationToken.None);
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (metrics.BlocksAccepted != 2 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        if (metrics.BlocksAccepted != 2 ||
            !submittedBlocks.Order().SequenceEqual(
                new[] { tempCandidate.BlockHex, finalCandidate.BlockHex }.Order()))
            throw new InvalidOperationException(
                "pending temp restart recovery did not submit the committed candidates exactly once");

        await recoveredQueue.StopAsync(TimeSpan.FromSeconds(2));
        if (File.Exists(tempPath) || File.Exists(tempPath[..^4]) ||
            File.Exists(finalPath) || File.Exists(finalPath + ".tmp"))
            throw new InvalidOperationException("accepted recovered pending files were not removed");
        if (!File.Exists(corruptPath) || !File.Exists(mismatchedPath) || !File.Exists(traversalPath))
            throw new InvalidOperationException("invalid pending file was treated as recoverable");

        Console.WriteLine("PASS pending temp restart recovery checks");
    }
    finally
    {
        if (recoveredQueue != null && !recoveredQueue.IsStopped)
            await recoveredQueue.StopAsync(TimeSpan.FromSeconds(2));
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunP2pHeaderReadAndTruncationChecksAsync()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-p2p-frame-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused");
        var metrics = new MetricsStore();
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        var peer = new P2pFastPeer(cfg, new TemplateEngine(cfg, rpc, metrics, queue), rpc);

        async Task<(string Command, byte[] Payload)> ReadFrameAsync(
            string command,
            byte[] payload,
            int? bytesToWrite = null)
        {
            using var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var endpoint = (IPEndPoint)tcpListener.LocalEndpoint;
            using var sender = new TcpClient();
            var connect = sender.ConnectAsync(endpoint.Address, endpoint.Port);
            using var receiver = await tcpListener.AcceptTcpClientAsync();
            await connect;
            tcpListener.Stop();

            var header = new byte[24];
            BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0, 4), 0xdab5bffa);
            Encoding.ASCII.GetBytes(command).CopyTo(header, 4);
            BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(16, 4), (uint)payload.Length);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var readTask = peer.ReadMessageAsync(receiver.GetStream(), cts.Token);
            var stream = sender.GetStream();
            await stream.WriteAsync(header, cts.Token);
            var writeLength = bytesToWrite ?? payload.Length;
            if (writeLength > 0)
                await stream.WriteAsync(payload.AsMemory(0, writeLength), cts.Token);
            sender.Client.Shutdown(SocketShutdown.Send);
            return await readTask;
        }

        var blockPayload = Enumerable.Range(0, 1024).Select(i => (byte)i).ToArray();
        var valid = await ReadFrameAsync("block", blockPayload);
        if (valid.Command != "block" || valid.Payload.Length != 80 ||
            !valid.Payload.AsSpan().SequenceEqual(blockPayload.AsSpan(0, 80)))
            throw new InvalidOperationException("P2P block frame header was not read and bounded");

        using (var tcpListener = new TcpListener(IPAddress.Loopback, 0))
        {
            tcpListener.Start();
            var endpoint = (IPEndPoint)tcpListener.LocalEndpoint;
            using var sender = new TcpClient();
            var connect = sender.ConnectAsync(endpoint.Address, endpoint.Port);
            using var receiver = await tcpListener.AcceptTcpClientAsync();
            await connect;
            tcpListener.Stop();

            var frameHeader = new byte[24];
            BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.AsSpan(0, 4), 0xdab5bffa);
            Encoding.ASCII.GetBytes("cmpctblock").CopyTo(frameHeader, 4);
            BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.AsSpan(16, 4), (uint)blockPayload.Length);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var earlyHeader = new TaskCompletionSource<(string Command, byte[] Header)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var readTask = peer.ReadMessageAsync(
                receiver.GetStream(),
                cts.Token,
                headerBytes =>
                {
                    earlyHeader.TrySetResult(("cmpctblock", headerBytes.ToArray()));
                    return Task.CompletedTask;
                });
            var stream = sender.GetStream();
            await stream.WriteAsync(frameHeader, cts.Token);
            await stream.WriteAsync(blockPayload.AsMemory(0, 80), cts.Token);

            var observed = await earlyHeader.Task.WaitAsync(cts.Token);
            if (observed.Command != "cmpctblock" || observed.Header.Length != 80 ||
                readTask.IsCompleted)
                throw new InvalidOperationException("compact-block header waited for the complete payload before dispatch");

            await stream.WriteAsync(blockPayload.AsMemory(80), cts.Token);
            sender.Client.Shutdown(SocketShutdown.Send);
            var checkedFrame = await readTask;
            if (checkedFrame.Command != "cmpctblock")
                throw new InvalidOperationException("compact-block frame did not complete after payload drain");
        }

        try
        {
            await ReadFrameAsync("ping", new byte[8], bytesToWrite: 4);
            throw new InvalidOperationException("truncated P2P payload was accepted");
        }
        catch (EndOfStreamException)
        {
            // Expected.
        }

        Console.WriteLine("PASS P2P header read and truncation checks");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunSafetyRegressionChecksAsync()
{
    RunHarnessModeChecks();
    RunBlockSubsidyBoundaryChecks();
    RunP2pFastGuardChecks();
    RunP2pFastLogFormatCheck();
    RunP2pPeerConfigurationChecks();
    RunChainGuardSyncChecks();
    RunBitcoinProtocolNegativeChecks();
    RunExtranonceLeaseChecks();
    RunWriterWakeSignalShutdownChecks();
    RunJobLookupSnapshotPublicationCheck();
    RunDifficultyBoundaryChecks();
    RunNetworkDifficultyClampChecks();
    RunWorkAssignmentChecks();
    await RunDifficultyAssignmentLifecycleChecksAsync();
    await RunPendingDifficultyTemplateSourceChecksAsync();
    await RunPendingTempRecoveryCheckAsync();
    await RunP2pHeaderReadAndTruncationChecksAsync();
    RunAcceptedShareTrackingIntegrationCheck();
    RunSubmitParserChecks();
    RunPooledResponseFrameChecks();
    RunGbtHexConverterChecks();
    RunTemplateFingerprintAllocationCheck();
    RunTemplateKeyIdentityCheck();
    RunMerkleRootCacheChecks();
    RunMetricsWindowChecks();
    await RunSubmitBlockContentChecksAsync();
    await RunInconclusiveRetryCheckAsync();
    await RunMissingParentRetryCheckAsync();
    RunDuplicateShareChecks();
    await RunServiceSupervisorChecksAsync();
    await RunRefreshFailureReadinessCheckAsync();
    await RunBlockOwnershipBeforeResponseCheckAsync();
    await RunDirectGbtDoesNotBlockFastPublishCheckAsync();
    RunGbtResponseOrderingChecks();
    await RunGbtRestartGenerationInvalidationCheckAsync();
    await RunDirectGbtBurstCoalescingCheckAsync();
    await RunLongpollDoesNotBlockDirectGbtCheckAsync();
    await RunCleanJobDispatchOrderingCheckAsync();
    RunLatestJobQueueCoalescingCheck();
    await RunClientJobWriteProgressCheckAsync();
    await RunPooledWriterFailureChecksAsync();
    RunRetainedTransactionBudgetCheck();
    await RunCrossSourceCleanLifecycleCheckAsync();
    Console.WriteLine("PASS safety regression checks");
}

static void RunWorkAssignmentChecks()
{
    var tracker = new WorkAssignmentTracker();
    var oldTarget = BitcoinEncoding.DiffToShareTargetLe(1);
    var newTarget = BitcoinEncoding.DiffToShareTargetLe(32);

    tracker.Register(
        issuedJobKey: 1,
        templateJobKey: 101,
        templateEpoch: 10,
        difficulty: 1,
        oldTarget);
    try
    {
        tracker.Register(
            issuedJobKey: 1,
            templateJobKey: 101,
            templateEpoch: 10,
            difficulty: 32,
            newTarget);
        throw new InvalidOperationException("conflicting work assignment was accepted");
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains(
               "conflicting assignment", StringComparison.Ordinal))
    {
        // Expected: immutable tokens must surface conflicting registration as an invariant failure.
    }

    if (!tracker.TryGet(1, out var oldAssignment) ||
        oldAssignment.TemplateJobKey != 101 ||
        oldAssignment.Difficulty != 1 ||
        !oldAssignment.ShareTargetLe.AsSpan().SequenceEqual(oldTarget))
        throw new InvalidOperationException("existing work token was reinterpreted at a new difficulty");

    tracker.Register(
        issuedJobKey: 2,
        templateJobKey: 102,
        templateEpoch: 11,
        difficulty: 32,
        newTarget);
    tracker.PruneBefore(11);
    if (tracker.TryGet(1, out _) ||
        !tracker.TryGet(2, out var retained) || retained.TemplateEpoch != 11 || tracker.Count != 1)
        throw new InvalidOperationException("expired template work assignments were not pruned");
    if (!tracker.TryGetRetiredTemplateKey(1, out var retiredPublicTemplate) ||
        retiredPublicTemplate != 101)
        throw new InvalidOperationException("pruned work did not retain bounded stale-job identity");

    tracker.Reset();
    for (var i = 0; i < WorkAssignmentTracker.Capacity; i++)
    {
        tracker.Register(
            issuedJobKey: (ulong)i + 1,
            templateJobKey: (ulong)i + 1,
            templateEpoch: i + 1,
            difficulty: 32,
            newTarget);
    }
    try
    {
        tracker.Register(
            issuedJobKey: (ulong)WorkAssignmentTracker.Capacity + 1,
            templateJobKey: (ulong)WorkAssignmentTracker.Capacity + 1,
            templateEpoch: WorkAssignmentTracker.Capacity + 1,
            difficulty: 32,
            newTarget);
        throw new InvalidOperationException("work assignment capacity overflow was accepted");
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains(
               "capacity reached", StringComparison.Ordinal))
    {
        // Expected: previously advertised work remains valid until epoch pruning.
    }
    if (tracker.Count != WorkAssignmentTracker.Capacity || !tracker.TryGet(1, out _) ||
        tracker.TryGet((ulong)WorkAssignmentTracker.Capacity + 1, out _))
        throw new InvalidOperationException("work assignment capacity did not fail closed");

    Console.WriteLine("PASS work assignment checks");
}

static async Task RunDifficultyAssignmentLifecycleChecksAsync()
{
    const double suggestedDifficulty = 1e-12;
    const double firstLongpollDifficulty = 2e-12;
    const double secondLongpollDifficulty = 3e-12;
    var root = Path.Combine(
        Path.GetTempPath(),
        "miningcore-btc-solo-difficulty-assignment-" + Guid.NewGuid().ToString("N"));
    StratumMinerClient? preAuthorizeClient = null;
    StratumMinerClient? p2pFastClient = null;
    StratumMinerClient? versionRollingClient = null;
    using var serverCts = new CancellationTokenSource();
    Task? serverTask = null;

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.ListenAddr = "127.0.0.1";
        cfg.Stratum.ListenPort = GetFreeTcpPort();
        cfg.Stratum.IdleTimeoutSecs = 0;
        cfg.Difficulty.Min = suggestedDifficulty;
        cfg.Difficulty.Max = 1;
        cfg.Difficulty.Default = 1;
        cfg.Validate();

        using var rpc = new BitcoinRpcClient(
            cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var metrics = new MetricsStore();
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        var engine = new TemplateEngine(cfg, rpc, metrics, queue);
        var server = new StratumServer(cfg, engine, metrics);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var builder = new JobBuilder(cfg);
        var p2pFastTip = new ChainTip
        {
            HashHex = new string('0', 64),
            Height = 1_000_000,
            MedianTimePast = now - 1,
            Nbits = 0x207fffff,
            Version = 0x20002000,
            Vbrequired = 0x00002000,
            TargetLe = BitcoinEncoding.CompactTargetToLe(0x207fffff),
            NetworkDifficulty = 4.656542373906925e-10
        };
        var job = builder.BuildEmptyFast(
            p2pFastTip,
            new string('1', 64),
            nextHeight: 1_000_001,
            estimatedMtp: now - 1,
            source: TemplateSource.P2pFast);
        publish.Invoke(engine, new object[] { job, true, false });
        if (engine.TryUseAuthoritativeJob(_ => { }))
            throw new InvalidOperationException("offline P2P-fast job unexpectedly became authoritative");

        serverTask = server.RunAsync(serverCts.Token);

        preAuthorizeClient = new StratumMinerClient();
        await preAuthorizeClient.ConnectAsync("127.0.0.1", cfg.Stratum.ListenPort);
        await preAuthorizeClient.SubscribeAsync("difficulty-preauth/1.0");
        var preAuthInitialDifficulty = await preAuthorizeClient.WaitForDifficultyAsync(
            TimeSpan.FromSeconds(5));
        var preAuthInitialJob = await preAuthorizeClient.WaitForJobAsync(TimeSpan.FromSeconds(5));
        if (preAuthInitialJob.JobId != job.JobId)
            throw new InvalidOperationException("subscribe did not receive the active public job");

        await preAuthorizeClient.SuggestDifficultyAsync(suggestedDifficulty);
        if (preAuthorizeClient.NotificationCount != 1 ||
            preAuthorizeClient.DifficultyNotificationCount != 1)
            throw new InvalidOperationException(
                "pending pre-authorization difficulty emitted same-template work");

        await preAuthorizeClient.AuthorizeAsync("preauth-worker", "x");
        await Task.Delay(100);
        if (preAuthorizeClient.NotificationCount != 1 ||
            preAuthorizeClient.DifficultyNotificationCount != 1)
            throw new InvalidOperationException(
                "authorization consumed pending difficulty without a new public template");

        p2pFastClient = new StratumMinerClient();
        await p2pFastClient.ConnectAsync("127.0.0.1", cfg.Stratum.ListenPort);
        var (p2pFastEn1, _) = await p2pFastClient.SubscribeAsync("difficulty-p2p-fast/1.0");
        var p2pFastInitialDifficulty = await p2pFastClient.WaitForDifficultyAsync(
            TimeSpan.FromSeconds(5));
        var p2pFastInitialJob = await p2pFastClient.WaitForJobAsync(TimeSpan.FromSeconds(5));
        await p2pFastClient.AuthorizeAsync("p2p-fast-worker", "x");

        await p2pFastClient.SuggestDifficultyAsync(firstLongpollDifficulty);
        await p2pFastClient.SuggestDifficultyAsync(secondLongpollDifficulty);
        if (p2pFastClient.NotificationCount != 1 ||
            p2pFastClient.DifficultyNotificationCount != 1)
            throw new InvalidOperationException(
                "replaced pending difficulty emitted same-template work");

        var longpollJob = builder.FromGbt(new GbtResponse
        {
            Version = 0x20002000,
            PreviousBlockhash = new string('2', 64),
            CoinbaseValue = 5_000_000_000,
            Target = "7fffff" + new string('0', 58),
            CurTime = checked(now + 1),
            Bits = "207fffff",
            Height = 1_000_002,
            Transactions = Array.Empty<GbtTx>(),
            CoinbaseAux = new GbtCoinbaseAux { Flags = "" },
            Mintime = checked(now + 1),
            Vbrequired = 0x00002000,
            SubmitOld = true
        }, TemplateSource.Longpoll);
        publish.Invoke(engine, new object[] { longpollJob, false, true });
        var preAuthAppliedDifficulty = await preAuthorizeClient.WaitForDifficultyAsync(
            value => Math.Abs(value - suggestedDifficulty) <= suggestedDifficulty * 1e-9,
            TimeSpan.FromSeconds(5));
        var preAuthPublicJob = await preAuthorizeClient.WaitForJobAsync(
            candidate => candidate.JobId == longpollJob.JobId,
            TimeSpan.FromSeconds(5));
        var p2pFastAppliedDifficulty = await p2pFastClient.WaitForDifficultyAsync(
            value => Math.Abs(value - secondLongpollDifficulty) <= secondLongpollDifficulty * 1e-9,
            TimeSpan.FromSeconds(5));
        var longpollPublicJob = await p2pFastClient.WaitForJobAsync(
            candidate => candidate.JobId == longpollJob.JobId,
            TimeSpan.FromSeconds(5));

        if (p2pFastInitialJob.JobId != job.JobId ||
            preAuthPublicJob.JobId != longpollJob.JobId ||
            longpollPublicJob.JobId != longpollJob.JobId ||
            preAuthPublicJob.NTime != longpollJob.NtimeHex ||
            longpollPublicJob.NTime != longpollJob.NtimeHex ||
            preAuthAppliedDifficulty != suggestedDifficulty ||
            p2pFastAppliedDifficulty != secondLongpollDifficulty)
        {
            throw new InvalidOperationException(
                "pending difficulty was not bound to the next public template");
        }

        var oldJobSeparatingShare = MineTargetSeparatingStratumShare(
            p2pFastInitialJob,
            p2pFastEn1,
            "00000002",
            BitcoinEncoding.DiffToShareTargetLe(p2pFastInitialDifficulty),
            BitcoinEncoding.DiffToShareTargetLe(p2pFastAppliedDifficulty));
        var oldJobError = await p2pFastClient.SubmitExpectErrorAsync(
            "p2p-fast-worker",
            p2pFastInitialJob.JobId,
            oldJobSeparatingShare.Extranonce2,
            oldJobSeparatingShare.Ntime,
            oldJobSeparatingShare.Nonce);
        if (!oldJobError.Contains("Low difficulty share", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("old public job target was reinterpreted");

        var longpollShare = MineNonBlockStratumShare(
            longpollPublicJob, p2pFastEn1, "00000003");
        if (!await p2pFastClient.SubmitAsync(
                "p2p-fast-worker",
                longpollPublicJob.JobId,
                longpollShare.Extranonce2,
                longpollShare.Ntime,
                longpollShare.Nonce))
            throw new InvalidOperationException("pending-difficulty public work was not accepted");

        Console.WriteLine(
            $"pending difficulty P2pFast={p2pFastInitialJob.JobId} " +
            $"Longpoll={longpollPublicJob.JobId} applied={p2pFastAppliedDifficulty:G6}");

        versionRollingClient = new StratumMinerClient();
        await versionRollingClient.ConnectAsync("127.0.0.1", cfg.Stratum.ListenPort);
        var (versionEn1, _) = await versionRollingClient.SubscribeAsync("version-required/1.0");
        await versionRollingClient.WaitForDifficultyAsync(TimeSpan.FromSeconds(5));
        var versionJob = await versionRollingClient.WaitForJobAsync(TimeSpan.FromSeconds(5));
        await versionRollingClient.AuthorizeAsync("version-worker", "x");
        var negotiatedMask = await versionRollingClient.ConfigureVersionRollingAsync(
            "1fffe000", includeMinBitCount: false);
        var notifiedMask = await versionRollingClient.WaitForVersionMaskAsync(TimeSpan.FromSeconds(5));
        await versionRollingClient.ConfigureWithoutExtensionsAsync();
        var repeatedMask = await versionRollingClient.WaitForVersionMaskAsync(TimeSpan.FromSeconds(5));
        var expectedMask = StratumServer.EffectiveVersionMask(0x1fffe000, job).ToString("x8");
        if (negotiatedMask != expectedMask || notifiedMask != expectedMask || repeatedMask != expectedMask ||
            expectedMask != "1fffc000")
            throw new InvalidOperationException("vbrequired bits were advertised as version-rollable");

        var versionShare = MineStratumJob(versionJob, versionEn1, maxNonces: 1024)
            ?? throw new InvalidOperationException("could not mine version-mask regression share");
        if (!await versionRollingClient.SubmitAsync(
                "version-worker",
                versionJob.JobId,
                versionShare.Extranonce2,
                versionShare.Ntime,
                versionShare.Nonce,
                versionBits: "00000000"))
            throw new InvalidOperationException("submit did not preserve the required version bit");

        var originalOut = Console.Out;
        using var rejectionLog = new StringWriter(
            System.Globalization.CultureInfo.InvariantCulture);
        string duplicateError;
        try
        {
            Console.SetOut(rejectionLog);
            duplicateError = await versionRollingClient.SubmitExpectErrorAsync(
                "version-worker",
                versionJob.JobId,
                versionShare.Extranonce2,
                versionShare.Ntime,
                versionShare.Nonce,
                versionBits: "00000000");
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        var rejectionText = rejectionLog.ToString();
        if (!duplicateError.Contains("Duplicate share", StringComparison.OrdinalIgnoreCase) ||
            !rejectionText.Contains("share rejected reason=duplicate_share", StringComparison.Ordinal) ||
            !rejectionText.Contains($"share_hash={versionShare.HashHex}", StringComparison.Ordinal) ||
            !rejectionText.Contains("share_target=", StringComparison.Ordinal) ||
            !rejectionText.Contains("assigned_diff=", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"detailed rejected-share log missing fields: {rejectionText}");
        }

        Console.WriteLine("PASS difficulty assignment lifecycle checks");
    }
    finally
    {
        preAuthorizeClient?.Dispose();
        p2pFastClient?.Dispose();
        versionRollingClient?.Dispose();
        serverCts.Cancel();
        if (serverTask != null)
        {
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) { }
        }
        try { Directory.Delete(root, recursive: true); } catch { }
    }

}

static async Task RunPendingDifficultyTemplateSourceChecksAsync()
{
    var sources = new[]
    {
        TemplateSource.Longpoll,
        TemplateSource.ZmqHashblock,
        TemplateSource.P2pFast
    };

    foreach (var firstSource in sources)
    {
        foreach (var secondSource in sources)
            await RunSourcePairAsync(firstSource, secondSource);
    }

    Console.WriteLine("PASS pending difficulty template source checks");

    static async Task RunSourcePairAsync(
        TemplateSource firstSource,
        TemplateSource secondSource)
    {
        const double firstDifficulty = 3e-10;
        const double secondDifficulty = 4e-10;
        const double thirdDifficulty = 2.5e-10;
        const double fourthDifficulty = 3.5e-10;
        const double regtestNetworkDifficulty = 4.656542373906925e-10;
        var root = Path.Combine(
            Path.GetTempPath(),
            $"miningcore-btc-solo-pending-source-{firstSource}-{secondSource}-" +
            Guid.NewGuid().ToString("N"));
        StratumMinerClient? client = null;
        using var serverCts = new CancellationTokenSource();
        Task? serverTask = null;

        try
        {
            var cfg = OfflineConfig(root);
            cfg.Stratum.ListenAddr = "127.0.0.1";
            cfg.Stratum.ListenPort = GetFreeTcpPort();
            cfg.Stratum.IdleTimeoutSecs = 0;
            cfg.Difficulty.Min = thirdDifficulty;
            cfg.Difficulty.Max = 1;
            cfg.Difficulty.Default = regtestNetworkDifficulty;
            cfg.Validate();

            using var rpc = new BitcoinRpcClient(
                cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
            var metrics = new MetricsStore();
            var queue = new BlockSubmitQueue(cfg, rpc, metrics);
            var engine = new TemplateEngine(cfg, rpc, metrics, queue);
            var server = new StratumServer(cfg, engine, metrics);
            var publish = typeof(TemplateEngine).GetMethod(
                "PublishJob",
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");
            var builder = new JobBuilder(cfg);
            var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            JobTemplate BuildSourceJob(TemplateSource source, int ordinal)
            {
                var ntime = checked(now + (uint)(ordinal - 1));
                var height = checked((uint)(100 + ordinal));
                var prevhash = new string(ordinal == 1 ? '1' : '2', 64);
                if (source == TemplateSource.P2pFast)
                {
                    var tip = new ChainTip
                    {
                        HashHex = new string('0', 64),
                        Height = height - 1,
                        MedianTimePast = ntime - 1,
                        Nbits = 0x207fffff,
                        Version = 0x20000000,
                        Vbrequired = 0,
                        TargetLe = BitcoinEncoding.CompactTargetToLe(0x207fffff),
                        NetworkDifficulty = regtestNetworkDifficulty
                    };
                    return builder.BuildEmptyFast(
                        tip, prevhash, height, ntime - 1, TemplateSource.P2pFast);
                }

                return builder.FromGbt(new GbtResponse
                {
                    Version = 0x20000000,
                    PreviousBlockhash = prevhash,
                    CoinbaseValue = 5_000_000_000,
                    Target = "7fffff" + new string('0', 58),
                    CurTime = ntime,
                    Bits = "207fffff",
                    Height = height,
                    Transactions = Array.Empty<GbtTx>(),
                    CoinbaseAux = new GbtCoinbaseAux { Flags = "" },
                    Mintime = ntime,
                    Vbrequired = 0,
                    SubmitOld = true
                }, source);
            }

            void Publish(JobTemplate job) =>
                publish.Invoke(
                    engine,
                    new object[] { job, true, job.Source != TemplateSource.P2pFast });

            var firstJob = BuildSourceJob(firstSource, 1);
            var secondJob = BuildSourceJob(secondSource, 2);
            var thirdJob = BuildSourceJob(firstSource, 3);
            var fourthJob = BuildSourceJob(secondSource, 4);
            if (firstJob.JobId != "1" || secondJob.JobId != "2" ||
                thirdJob.JobId != "3" || fourthJob.JobId != "4")
                throw new InvalidOperationException(
                    $"public job ids changed for {firstSource}->{secondSource}");

            Publish(firstJob);
            serverTask = server.RunAsync(serverCts.Token);

            var miner = new StratumMinerClient();
            client = miner;
            await miner.ConnectAsync("127.0.0.1", cfg.Stratum.ListenPort);
            await miner.SubscribeAsync($"pending-source/{firstSource}-{secondSource}");
            await miner.WaitForDifficultyAsync(TimeSpan.FromSeconds(5));
            var firstPublic = await miner.WaitForJobAsync(TimeSpan.FromSeconds(5));
            await miner.AuthorizeAsync("source-worker", "x");
            AssertPublicJob(firstPublic, firstJob, firstSource);

            await miner.SuggestDifficultyAsync(firstDifficulty);
            await miner.SuggestDifficultyAsync(secondDifficulty);
            AssertNotificationCounts(miner, jobs: 1, difficulties: 1);

            Publish(secondJob);
            var appliedSecond = await miner.WaitForDifficultyAsync(
                value => Math.Abs(value - secondDifficulty) <= secondDifficulty * 1e-9,
                TimeSpan.FromSeconds(5));
            var secondPublic = await miner.WaitForJobAsync(
                candidate => candidate.JobId == secondJob.JobId,
                TimeSpan.FromSeconds(5));
            AssertPublicJob(secondPublic, secondJob, secondSource);
            AssertNotificationCounts(miner, jobs: 2, difficulties: 2);

            await miner.SuggestDifficultyAsync(thirdDifficulty);
            await miner.SuggestDifficultyAsync(fourthDifficulty);
            AssertNotificationCounts(miner, jobs: 2, difficulties: 2);

            Publish(thirdJob);
            var appliedFourth = await miner.WaitForDifficultyAsync(
                value => Math.Abs(value - fourthDifficulty) <= fourthDifficulty * 1e-9,
                TimeSpan.FromSeconds(5));
            var thirdPublic = await miner.WaitForJobAsync(
                candidate => candidate.JobId == thirdJob.JobId,
                TimeSpan.FromSeconds(5));
            AssertPublicJob(thirdPublic, thirdJob, firstSource);
            AssertNotificationCounts(miner, jobs: 3, difficulties: 3);

            await miner.SuggestDifficultyAsync(thirdDifficulty);
            await miner.SuggestDifficultyAsync(fourthDifficulty);
            AssertNotificationCounts(miner, jobs: 3, difficulties: 3);

            Publish(fourthJob);
            var fourthPublic = await miner.WaitForJobAsync(
                candidate => candidate.JobId == fourthJob.JobId,
                TimeSpan.FromSeconds(5));
            AssertPublicJob(fourthPublic, fourthJob, secondSource);
            AssertNotificationCounts(miner, jobs: 4, difficulties: 3);

            Console.WriteLine(
                $"pending source pair {firstSource}->{secondSource} " +
                $"public={firstPublic.JobId},{secondPublic.JobId}," +
                $"{thirdPublic.JobId},{fourthPublic.JobId} " +
                $"difficulty={appliedSecond:G6},{appliedFourth:G6}");
        }
        finally
        {
            client?.Dispose();
            serverCts.Cancel();
            if (serverTask != null)
            {
                try { await serverTask.WaitAsync(TimeSpan.FromSeconds(5)); }
                catch (OperationCanceledException) { }
            }
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    static void AssertPublicJob(
        StratumJob actual,
        JobTemplate expected,
        TemplateSource expectedSource)
    {
        if (actual.JobId != expected.JobId || expected.Source != expectedSource ||
            !MatchesJobTemplate(actual, expected))
        {
            throw new InvalidOperationException(
                $"{expectedSource} public job {expected.JobId} was not inherited");
        }
    }

    static void AssertNotificationCounts(
        StratumMinerClient client,
        int jobs,
        int difficulties)
    {
        if (client.NotificationCount != jobs ||
            client.DifficultyNotificationCount != difficulties)
            throw new InvalidOperationException(
                $"unexpected pending-difficulty notifications jobs={client.NotificationCount}/{jobs} " +
                $"difficulties={client.DifficultyNotificationCount}/{difficulties}");
    }

    static bool MatchesJobTemplate(StratumJob actual, JobTemplate expected) =>
        actual.PrevHash == expected.PrevhashNotifyHex &&
        actual.Coinbase1 == expected.Coinbase1Hex &&
        actual.Coinbase2 == expected.Coinbase2Hex &&
        actual.MerkleBranch.SequenceEqual(expected.MerkleBranchesHex) &&
        actual.Version == expected.VersionHex &&
        actual.NBits == expected.NbitsHex &&
        actual.NTime == expected.NtimeHex;

}

static void RunSubmitParserChecks()
{
    var json = Encoding.UTF8.GetBytes(
        "{\"id\":17,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1a\",\"00000001\",\"65abcdef\",\"89abcdef\",\"20000000\"]}");
    var sequence = TestSequenceSegment.Create(
        json.AsMemory(0, 7),
        json.AsMemory(7, 19),
        json.AsMemory(26, 31),
        json.AsMemory(57));
    if (!StratumServer.TryParseSubmit(sequence, out var submit) ||
        !submit.HasRequiredParams || submit.Id.Kind != StratumRequestIdKind.Int64 ||
        submit.Id.Signed != 17 || submit.JobKey != 0x1a ||
        submit.Extranonce2 != 1 || submit.Extranonce2HexLength != 8 ||
        submit.Ntime != 0x65abcdef || submit.Nonce != 0x89abcdef ||
        !submit.HasVersion || submit.Version != 0x20000000)
        throw new InvalidOperationException("segmented UTF-8 submit parsing changed");

    if (StratumServer.HasValidVersionParameterShape(
            versionRolling: false, hasVersion: true, versionValid: true) ||
        !StratumServer.HasValidVersionParameterShape(
            versionRolling: false, hasVersion: false, versionValid: false) ||
        !StratumServer.HasValidVersionParameterShape(
            versionRolling: true, hasVersion: true, versionValid: true) ||
        StratumServer.HasValidVersionParameterShape(
            versionRolling: true, hasVersion: false, versionValid: false))
    {
        throw new InvalidOperationException("version submit parameter gating changed");
    }

    var maximumPublicJob = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"7fffffffffffffff\",\"1\",\"1\",\"2\"]}"));
    if (!StratumServer.TryParseSubmit(maximumPublicJob, out var maximumPublicSubmit) ||
        !maximumPublicSubmit.HasRequiredParams ||
        maximumPublicSubmit.JobKey != long.MaxValue)
        throw new InvalidOperationException("maximum public work token did not parse");

    var invalidVersion = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1a\",\"00000001\",\"65abcdef\",\"89abcdef\",\"not-hex\"]}"));
    if (!StratumServer.TryParseSubmit(invalidVersion, out var invalidVersionSubmit) ||
        !invalidVersionSubmit.HasRequiredParams || !invalidVersionSubmit.HasVersion ||
        invalidVersionSubmit.VersionValid)
        throw new InvalidOperationException("malformed optional version was not identified");

    foreach (var malformedVersion in new[] { "1", "0x20000000", " 20000000 " })
    {
        var malformedMask = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
            $"{{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1a\",\"00000001\",\"65abcdef\",\"89abcdef\",\"{malformedVersion}\"]}}"));
        if (!StratumServer.TryParseSubmit(malformedMask, out var malformedMaskSubmit) ||
            !malformedMaskSubmit.HasRequiredParams || malformedMaskSubmit.VersionValid)
        {
            throw new InvalidOperationException(
                $"non-TMask version_bits was accepted: '{malformedVersion}'");
        }
    }

    var extraSubmitParam = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1a\",\"00000001\",\"65abcdef\",\"89abcdef\",\"20000000\",\"extra\"]}"));
    if (!StratumServer.TryParseSubmit(extraSubmitParam, out var extraParamSubmit) ||
        extraParamSubmit.HasRequiredParams)
        throw new InvalidOperationException("mining.submit accepted more than six parameters");

    if (!StratumServer.TryParseVersionRollingMask("1fffe000", out var parsedMask) ||
        parsedMask != 0x1fffe000 ||
        StratumServer.TryParseVersionRollingMask("1", out _) ||
        StratumServer.TryParseVersionRollingMask("0x1fffe000", out _) ||
        StratumServer.TryParseVersionRollingMask(" 1fffe000", out _))
    {
        throw new InvalidOperationException("BIP310 TMask parsing is not exact");
    }

    static JsonElement VersionRollingParameters(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
    const uint serverMask = 0x1fffe000;
    if (!StratumServer.TryReadVersionRollingParameters(
            VersionRollingParameters(
                "{\"version-rolling.mask\":\"1ffff000\",\"version-rolling.min-bit-count\":100}"),
            serverMask,
            out var negotiatedMask) || negotiatedMask != serverMask ||
        !StratumServer.TryReadVersionRollingParameters(
            VersionRollingParameters("{\"version-rolling.mask\":\"1fffe000\"}"),
            serverMask,
            out var defaultMinBitCountMask) || defaultMinBitCountMask != serverMask ||
        StratumServer.TryReadVersionRollingParameters(
            VersionRollingParameters(
                "{\"version-rolling.mask\":\"1fffe000\",\"version-rolling.min-bit-count\":-1}"),
            serverMask,
            out _) ||
        StratumServer.TryReadVersionRollingParameters(
            VersionRollingParameters(
                "{\"version-rolling.mask\":\"1\",\"version-rolling.min-bit-count\":0}"),
            serverMask,
            out _))
    {
        throw new InvalidOperationException("version-rolling parameter validation changed");
    }

    var stringId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"method\":\"mining.submit\",\"id\":\"request-7\",\"params\":[null,\"2\",\"0x01\",\"1\",\"2\"]}"));
    if (!StratumServer.TryParseSubmit(stringId, out var stringSubmit) ||
        stringSubmit.Id.Kind != StratumRequestIdKind.String || stringSubmit.Id.Text != "request-7" ||
        stringSubmit.Extranonce2 != 1 || stringSubmit.Extranonce2HexLength != 2)
        throw new InvalidOperationException("string-id submit compatibility changed");

    var nullId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":null,\"method\":\"mining.submit\",\"params\":[null,\"3\",\"\",\"1\",\"2\"]}"));
    if (!StratumServer.TryParseSubmit(nullId, out var nullSubmit) ||
        nullSubmit.Id.Kind != StratumRequestIdKind.Null || !nullSubmit.Extranonce2Valid)
        throw new InvalidOperationException("null-id/empty-extranonce submit compatibility changed");

    var invalidJobId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1g\",\"00000001\",\"65abcdef\",\"89abcdef\"]}"));
    if (!StratumServer.TryParseSubmit(invalidJobId, out var invalidJobSubmit) ||
        invalidJobSubmit.HasRequiredParams)
        throw new InvalidOperationException("malformed job id was accepted as submit params");

    var objectSubmitParams = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":19,\"method\":\"mining.submit\"}"));
    if (!StratumServer.TryParseSubmit(objectSubmitParams, out var objectSubmit) ||
        objectSubmit.HasRequiredParams || objectSubmit.Id.Kind != StratumRequestIdKind.Int64 ||
        objectSubmit.Id.Signed != 19)
        throw new InvalidOperationException("object submit params changed fast-parser compatibility");

    var objectNonSubmitParams = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":20,\"method\":\"mining.subscribe\"}"));
    if (StratumServer.TryParseSubmit(objectNonSubmitParams, out _))
        throw new InvalidOperationException("non-submit object params were classified as a submit");

    var segmentedObjectBytes = Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":21,\"method\":\"mining.submit\"}");
    var segmentedObjectParams = TestSequenceSegment.Create(
        segmentedObjectBytes.AsMemory(0, 13),
        segmentedObjectBytes.AsMemory(13, 17),
        segmentedObjectBytes.AsMemory(30));
    if (!StratumServer.TryParseSubmit(segmentedObjectParams, out var segmentedObjectSubmit) ||
        segmentedObjectSubmit.HasRequiredParams || segmentedObjectSubmit.Id.Signed != 21)
        throw new InvalidOperationException("segmented object submit params changed parser compatibility");

    static void RequireMalformedSubmitRejected(string malformed, string description)
    {
        try
        {
            _ = StratumServer.TryParseSubmit(
                new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(malformed)), out _);
            throw new InvalidOperationException($"{description} was accepted");
        }
        catch (JsonException)
        {
            // expected
        }
    }

    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":[\"worker\",\"1\",\"1\",\"1\",\"1\"]} trailing-garbage",
        "submit JSON with trailing garbage");
    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":[\"worker\",\"1\",\"1\",\"1\",\"1\"]",
        "unterminated submit root object");
    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":{\"nested\":[1,2]}",
        "unterminated submit object params");

    var contiguous = new ReadOnlySequence<byte>(json);
    for (var i = 0; i < 32; i++)
        _ = StratumServer.TryParseSubmit(contiguous, out _);
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    for (var i = 0; i < 10_000; i++)
    {
        if (!StratumServer.TryParseSubmit(contiguous, out _))
            throw new InvalidOperationException("submit parser stopped recognizing the hot-path request");
    }
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (allocated != 0)
        throw new InvalidOperationException($"numeric submit parser allocated {allocated} bytes");
}

static void RunPooledResponseFrameChecks()
{
    static string Consume(PooledFrameBuffer frame)
    {
        try
        {
            return Encoding.UTF8.GetString(frame.Buffer, 0, frame.Length);
        }
        finally
        {
            frame.Return();
        }
    }

    var baseline = OutboundFrame.OutstandingPooledBufferCount;
    var numeric = Consume(StratumServer.BuildPooledStratumErrorFrame(
        StratumRequestId.FromInt64(17), 23, "Low difficulty share"));
    var text = Consume(StratumServer.BuildPooledOkTrueFrame(
        StratumRequestId.FromString("request-7")));
    var nullId = Consume(StratumServer.BuildPooledOkTrueFrame(StratumRequestId.Null));
    if (numeric != "{\"id\":17,\"result\":false,\"error\":[23,\"Low difficulty share\",null]}\n" ||
        text != "{\"id\":\"request-7\",\"result\":true,\"error\":null}\n" ||
        nullId != "{\"id\":null,\"result\":true,\"error\":null}\n")
        throw new InvalidOperationException("pooled submit response JSON semantics changed");

    for (var i = 0; i < 32; i++)
        StratumServer.BuildPooledStratumErrorFrame(
            StratumRequestId.FromInt64(i), 23, "Low difficulty share").Return();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    for (var i = 0; i < 10_000; i++)
        StratumServer.BuildPooledStratumErrorFrame(
            StratumRequestId.FromInt64(i), 23, "Low difficulty share").Return();
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (allocated != 0)
        throw new InvalidOperationException($"numeric pooled error builder allocated {allocated} bytes");
    if (OutboundFrame.OutstandingPooledBufferCount != baseline)
        throw new InvalidOperationException("response builder did not return all pooled buffers");
}

static void RunGbtHexConverterChecks()
{
    var txid = new string('1', 64);
    var ordered = JsonSerializer.Deserialize<GbtTx>(
        $"{{\"txid\":\"{txid}\",\"hash\":\"{txid}\",\"data\":\"00aBcd\"}}")
        ?? throw new InvalidOperationException("GBT transaction did not deserialize");
    if (!ordered.Data.SequenceEqual(new byte[] { 0x00, 0xab, 0xcd }) || ordered.TxId != txid)
        throw new InvalidOperationException("GBT transaction field-order/hex decoding changed");

    const int decodedLength = 100_000;
    var token = new byte[decodedLength * 2 + 2];
    token[0] = (byte)'\"';
    token[^1] = (byte)'\"';
    for (var i = 1; i < token.Length - 1; i += 2)
    {
        token[i] = (byte)'a';
        token[i + 1] = (byte)'5';
    }
    var sequence = TestSequenceSegment.Create(
        token.AsMemory(0, 8191),
        token.AsMemory(8191, 65537),
        token.AsMemory(73728));
    var reader = new Utf8JsonReader(sequence, isFinalBlock: true, state: default);
    if (!reader.Read())
        throw new InvalidOperationException("large GBT hex fixture was empty");
    var converter = new HexByteArrayJsonConverter();
    var converterOptions = new JsonSerializerOptions();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var decoded = converter.Read(ref reader, typeof(byte[]), converterOptions);
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (decoded.Length != decodedLength || decoded[0] != 0xa5 || decoded[^1] != 0xa5)
        throw new InvalidOperationException("segmented large GBT transaction decoded incorrectly");
    if (allocated > decodedLength + 32_768)
        throw new InvalidOperationException(
            $"large GBT transaction decode allocated {allocated} bytes for {decodedLength} output bytes");

    var malformed = new Utf8JsonReader("\"00xz\""u8, isFinalBlock: true, state: default);
    malformed.Read();
    try
    {
        converter.Read(ref malformed, typeof(byte[]), new JsonSerializerOptions());
        throw new InvalidOperationException("malformed GBT transaction hex was accepted");
    }
    catch (JsonException)
    {
        // expected
    }

    var packedEmpty = JsonSerializer.Deserialize<GbtResponse>("{\"transactions\":[]}")
        ?? throw new InvalidOperationException("empty packed GBT did not deserialize");
    packedEmpty.Transactions = null!;
    using var packedEmptyJson = JsonDocument.Parse(JsonSerializer.Serialize(packedEmpty));
    if (packedEmptyJson.RootElement.GetProperty("transactions").GetArrayLength() != 0)
        throw new InvalidOperationException("empty packed GBT did not serialize as an empty array");
}

static void RunTemplateFingerprintAllocationCheck()
{
    var hashes = new byte[4_000 * 32];
    for (var i = 0; i < 4_000; i++)
        for (var j = 0; j < 32; j++)
            hashes[i * 32 + j] = (byte)(i + j);
    JobBuilder.TxSetFingerprint(hashes);
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var fingerprint = JobBuilder.TxSetFingerprint(hashes);
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (fingerprint.Length != 32)
        throw new InvalidOperationException("transaction-set fingerprint length changed");
    if (allocated >= 85_000)
        throw new InvalidOperationException($"transaction-set fingerprint created a LOH allocation ({allocated} bytes)");
}

static void RunTemplateKeyIdentityCheck()
{
    var gbt = new GbtResponse
    {
        Version = 0x20000000,
        PreviousBlockhash = new string('1', 64),
        CoinbaseValue = 5_000_000_000,
        Target = "7fffff" + new string('0', 58),
        CurTime = 1_700_000_000,
        Bits = "207fffff",
        Height = 1_000_000,
        Transactions = new[]
        {
            new GbtTx { Data = new byte[] { 1 }, TxId = new string('2', 64), Hash = new string('2', 64) },
            new GbtTx { Data = new byte[] { 2 }, TxId = new string('3', 64), Hash = new string('3', 64) }
        }
    };
    var builder = new JobBuilder(OfflineConfig(Path.GetTempPath()));
    var expected = builder.ComputeTemplateKeyParts(gbt).Key;
    var direct = builder.FromGbt(gbt, TemplateSource.Startup);
    if (!string.Equals(direct.TemplateKey, expected, StringComparison.Ordinal))
        throw new InvalidOperationException("Merkle reduction changed the transaction-set template key");

    static GbtResponse Copy(GbtResponse source) => new()
    {
        Version = source.Version,
        PreviousBlockhash = source.PreviousBlockhash,
        CoinbaseValue = source.CoinbaseValue,
        Target = source.Target,
        CurTime = source.CurTime,
        Bits = source.Bits,
        Height = source.Height,
        Transactions = source.Transactions,
        CoinbaseAux = source.CoinbaseAux == null
            ? null
            : new GbtCoinbaseAux { Flags = source.CoinbaseAux.Flags },
        DefaultWitnessCommitment = source.DefaultWitnessCommitment,
        LongPollId = source.LongPollId,
        Mintime = source.Mintime,
        Vbrequired = source.Vbrequired,
        SubmitOld = source.SubmitOld
    };

    var mutations = new (string Name, Action<GbtResponse> Apply)[]
    {
        ("version", value => value.Version++),
        ("vbrequired", value => value.Vbrequired = 0x20000000),
        ("target/bits", value =>
        {
            value.Bits = "2070ffff";
            value.Target = Hex.Encode(Hex.ReverseCopy(
                BitcoinEncoding.CompactTargetToLe(0x2070ffff)));
        }),
        ("curtime", value => value.CurTime++),
        ("mintime", value => value.Mintime = value.CurTime > 0 ? value.CurTime - 1 : 0),
        ("coinbaseaux.flags", value => value.CoinbaseAux = new GbtCoinbaseAux { Flags = "01aa" }),
        ("submitold", value => value.SubmitOld = false),
        ("witness commitment", value => value.DefaultWitnessCommitment = "6a24aa21a9ed" + new string('0', 64))
    };
    foreach (var mutation in mutations)
    {
        var changed = Copy(gbt);
        mutation.Apply(changed);
        var changedKey = builder.ComputeTemplateKeyParts(changed).Key;
        if (string.Equals(expected, changedKey, StringComparison.Ordinal))
            throw new InvalidOperationException($"template key ignored {mutation.Name}");
    }
}

static async Task RunSubmitBlockContentChecksAsync()
{
    var blockHex = string.Concat(Enumerable.Repeat("00abcdef", 1024));
    using (var content = SubmitBlockRpcContent.Create(42, blockHex))
    {
        var payload = await content.ReadAsByteArrayAsync();
        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        if (root.GetProperty("jsonrpc").GetString() != "1.0" ||
            root.GetProperty("id").GetString() != "42" ||
            root.GetProperty("method").GetString() != "submitblock" ||
            root.GetProperty("params")[0].GetString() != blockHex)
            throw new InvalidOperationException("submitblock UTF-8 payload changed JSON-RPC semantics");
        if (content.Headers.ContentLength != payload.Length)
            throw new InvalidOperationException("submitblock content length was not deterministic");
    }

    var binaryBytes = new byte[131_073];
    for (var i = 0; i < binaryBytes.Length; i++)
        binaryBytes[i] = (byte)(i * 37 + 11);
    var binaryCandidate = new BlockCandidate(binaryBytes);
    var binaryHex = Hex.Encode(binaryBytes);
    using (var content = SubmitBlockRpcContent.Create(43, binaryCandidate.Bytes))
    {
        var payload = await content.ReadAsByteArrayAsync();
        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        if (root.GetProperty("jsonrpc").GetString() != "1.0" ||
            root.GetProperty("id").GetString() != "43" ||
            root.GetProperty("method").GetString() != "submitblock" ||
            root.GetProperty("params")[0].GetString() != binaryHex)
            throw new InvalidOperationException("binary submitblock payload changed JSON-RPC or hex bytes");
        if (content.Headers.ContentLength != payload.Length)
            throw new InvalidOperationException("binary submitblock content length was not deterministic");
    }

    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    var serverTask = Task.Run(async () =>
    {
        var expected = new[] { blockHex, binaryHex };
        for (var i = 0; i < expected.Length; i++)
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("params")[0].GetString() != expected[i])
                throw new InvalidOperationException("submitblock HTTP body was truncated or changed");
            var response = Encoding.UTF8.GetBytes(
                $"{{\"result\":null,\"error\":null,\"id\":\"{i + 1}\"}}");
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();
        }
    });

    using var rpc = new BitcoinRpcClient($"http://127.0.0.1:{port}", "user", "pass", requestTimeoutSecs: 15);
    var result = await rpc.SubmitBlockAsync(blockHex);
    var binaryResult = await rpc.SubmitBlockAsync(binaryCandidate);
    await serverTask;
    listener.Stop();
    if (result != null || binaryResult != null)
        throw new InvalidOperationException("accepted submitblock response was not parsed as null");

    var malformedPort = GetFreeTcpPort();
    using var malformedListener = new HttpListener();
    malformedListener.Prefixes.Add($"http://127.0.0.1:{malformedPort}/");
    malformedListener.Start();
    var malformedServerTask = Task.Run(async () =>
    {
        var context = await malformedListener.GetContextAsync();
        var response = Encoding.UTF8.GetBytes("{\"error\":null,\"id\":\"2\"}");
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    });

    using var malformedRpc = new BitcoinRpcClient(
        $"http://127.0.0.1:{malformedPort}", "user", "pass", requestTimeoutSecs: 15);
    var missingResultRejected = false;
    try
    {
        await malformedRpc.SubmitBlockAsync(blockHex);
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("missing required 'result'", StringComparison.Ordinal))
    {
        missingResultRejected = true;
    }
    await malformedServerTask;
    malformedListener.Stop();
    if (!missingResultRejected)
        throw new InvalidOperationException("submitblock response without result was accepted");
}

static async Task RunMissingParentRetryCheckAsync()
{
    if (!BlockSubmitQueue.IsMissingParentResult("prev-blk-not-found") ||
        BlockSubmitQueue.IsMissingParentResult("bad-prevblk"))
        throw new InvalidOperationException("submitblock missing-parent classification is incorrect");

    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-parent-retry-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();

    var firstRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var secondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("method").GetString() != "submitblock")
                throw new InvalidOperationException("parent retry server received a non-submitblock request");

            var json = attempt == 1
                ? "{\"result\":\"prev-blk-not-found\",\"error\":null,\"id\":\"1\"}"
                : "{\"result\":null,\"error\":null,\"id\":\"2\"}";
            var response = Encoding.UTF8.GetBytes(json);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();

            if (attempt == 1)
                firstRequest.TrySetResult();
            else
                secondRequest.TrySetResult();
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var metrics = new MetricsStore();
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        await queue.StartAsync(CancellationToken.None);
        var candidate = CreateQueueTestBlock(2);
        await queue.EnqueueFoundBlockAsync(candidate.BlockHex, candidate.Hash, 900_002);

        await firstRequest.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var wakeLatency = Stopwatch.StartNew();
        queue.NotifyChainStateChanged();
        await secondRequest.Task.WaitAsync(TimeSpan.FromSeconds(2));
        wakeLatency.Stop();
        if (wakeLatency.ElapsedMilliseconds >= 180)
            throw new InvalidOperationException(
                $"chain-state signal did not wake submitblock promptly ({wakeLatency.ElapsedMilliseconds}ms)");
        await serverTask.WaitAsync(TimeSpan.FromSeconds(2));

        var acceptedDeadline = DateTime.UtcNow.AddSeconds(2);
        while (metrics.BlocksAccepted == 0 && DateTime.UtcNow < acceptedDeadline)
            await Task.Delay(10);
        if (metrics.BlocksAccepted != 1)
            throw new InvalidOperationException("missing-parent block was not accepted after chain-state wakeup");

        var failedDir = Path.Combine(root, "failed-blocks");
        if (Directory.Exists(failedDir) && Directory.GetFiles(failedDir, "*.json").Length != 0)
            throw new InvalidOperationException("missing-parent block was archived as a permanent rejection");

        await queue.StopAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunInconclusiveRetryCheckAsync()
{
    if (!BlockSubmitQueue.IsInconclusiveResult("inconclusive") ||
        !BlockSubmitQueue.IsInconclusiveResult("DUPLICATE-INCONCLUSIVE") ||
        BlockSubmitQueue.IsInconclusiveResult("duplicate-invalid"))
        throw new InvalidOperationException("submitblock inconclusive classification is incorrect");

    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-inconclusive-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();

    var firstResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var secondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var allowAcceptance = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("method").GetString() != "submitblock")
                throw new InvalidOperationException("inconclusive retry server received a non-submitblock request");

            if (attempt == 2)
            {
                secondRequest.TrySetResult();
                await allowAcceptance.Task;
            }

            var json = attempt == 1
                ? "{\"result\":\"inconclusive\",\"error\":null,\"id\":\"1\"}"
                : "{\"result\":null,\"error\":null,\"id\":\"2\"}";
            var response = Encoding.UTF8.GetBytes(json);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();

            if (attempt == 1)
                firstResponse.TrySetResult();
        }
    });

    BlockSubmitQueue? queue = null;
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var metrics = new MetricsStore();
        queue = new BlockSubmitQueue(cfg, rpc, metrics);
        await queue.StartAsync(CancellationToken.None);
        var candidate = CreateQueueTestBlock(3);
        await queue.EnqueueFoundBlockAsync(candidate.BlockHex, candidate.Hash, 900_003);

        await firstResponse.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await secondRequest.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var pending = Directory.GetFiles(queue.PendingDir, "*.json");
        if (pending.Length != 1)
            throw new InvalidOperationException(
                $"inconclusive submit persisted {pending.Length} candidates, expected 1");
        var failedDir = Path.Combine(root, "failed-blocks");
        if (Directory.GetFiles(failedDir, "*.json").Length != 0)
            throw new InvalidOperationException("inconclusive submit was archived as a permanent rejection");
        if (!metrics.GetBlocks().Any(x => x.result == "inconclusive"))
            throw new InvalidOperationException("inconclusive submit status was not recorded");

        allowAcceptance.TrySetResult();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        var acceptedDeadline = DateTime.UtcNow.AddSeconds(2);
        while (metrics.BlocksAccepted == 0 && DateTime.UtcNow < acceptedDeadline)
            await Task.Delay(10);
        if (metrics.BlocksAccepted != 1)
            throw new InvalidOperationException("inconclusive candidate was not accepted after retry");
        var finalBlockEvents = metrics.GetBlocks();
        if (metrics.BlocksSubmitted != 1 || finalBlockEvents.Count != 1 ||
            finalBlockEvents[0].result != "submitted")
            throw new InvalidOperationException(
                "inconclusive-to-accepted transition double-counted the candidate or kept a stale status");
        if (Directory.GetFiles(queue.PendingDir, "*.json").Length != 0)
            throw new InvalidOperationException("accepted inconclusive candidate remained pending");

        await queue.StopAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        allowAcceptance.TrySetResult();
        if (queue != null && !queue.IsStopped)
            await queue.StopAsync(TimeSpan.FromSeconds(2));
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static int GetFreeTcpPort()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
    listener.Start();
    var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}

static void RunMetricsWindowChecks()
{
    const int sampleCount = 50_017;
    const string sampleShareHash =
        "00000000000000000000caf03d17f697d83499b661191fd1c3ea332b7dc1bb57";
    var metrics = new MetricsStore();
    var identity = new WorkerIdentity
    {
        SessionId = "metrics-window",
        Name = "worker",
        UserAgent = "offline-check",
        Peer = "127.0.0.1:1",
        Extranonce1 = "00000001",
        IsNormalized = true
    };

    for (var i = 0; i < sampleCount; i++)
    {
        metrics.RecordShare(
            identity, 1, 1, accepted: true, assignedDifficulty: 32,
            hash: sampleShareHash);
    }

    var expectedHps = sampleCount * 4294967296.0 / (MetricsStore.HashrateMinWindowMs / 1000.0);
    var totalHps = metrics.EstimateTotalHps();
    var workerAllocationBefore = GC.GetAllocatedBytesForCurrentThread();
    var workers = metrics.GetWorkers();
    var workerAllocation = GC.GetAllocatedBytesForCurrentThread() - workerAllocationBefore;
    if (Math.Abs(totalHps - expectedHps) / expectedHps > 1e-12)
        throw new InvalidOperationException($"metrics total hashrate changed: {totalHps} != {expectedHps}");
    if (workers.Count != 1 || Math.Abs(workers[0].hashrate_hps - expectedHps) / expectedHps > 1e-12)
        throw new InvalidOperationException("worker hashrate aggregation diverged from total hashrate");
    var recentShares = metrics.GetShares();
    if (recentShares.Count != 15 || recentShares.Any(x => x.hash != sampleShareHash))
        throw new InvalidOperationException("recent share dashboard hashes were not retained");
    if (workerAllocation >= 85_000)
        throw new InvalidOperationException($"worker dashboard snapshot created a LOH allocation ({workerAllocation} bytes)");

    long boundaryNowMs = 1_000_004_999;
    var boundaryMetrics = new MetricsStore(() => boundaryNowMs);
    var boundaryIdentity = new WorkerIdentity
    {
        SessionId = "metrics-boundary",
        Name = "worker",
        UserAgent = "offline-check",
        Peer = "127.0.0.1:2",
        Extranonce1 = "00000002",
        IsNormalized = true
    };
    boundaryMetrics.RecordShare(
        boundaryIdentity, 1, 1, accepted: true, assignedDifficulty: 32);
    boundaryNowMs += MetricsStore.HashrateWindowMs;
    boundaryMetrics.RecordShare(
        boundaryIdentity, 2, 2, accepted: true, assignedDifficulty: 32);

    var expectedBoundaryHps = 3 * 4294967296.0 /
        (MetricsStore.HashrateWindowMs / 1000.0);
    var boundaryTotalHps = boundaryMetrics.EstimateTotalHps();
    var boundaryWorkers = boundaryMetrics.GetWorkers();
    if (Math.Abs(boundaryTotalHps - expectedBoundaryHps) / expectedBoundaryHps > 1e-12 ||
        boundaryWorkers.Count != 1 ||
        Math.Abs(boundaryWorkers[0].hashrate_hps - expectedBoundaryHps) /
            expectedBoundaryHps > 1e-12)
        throw new InvalidOperationException(
            "partially live hashrate boundary bucket was evicted early");

    boundaryNowMs += 1_001;
    var expectedAgedHps = 2 * 4294967296.0 /
        (MetricsStore.HashrateMinWindowMs / 1000.0);
    var agedTotalHps = boundaryMetrics.EstimateTotalHps();
    var agedWorkers = boundaryMetrics.GetWorkers();
    if (Math.Abs(agedTotalHps - expectedAgedHps) / expectedAgedHps > 1e-12 ||
        agedWorkers.Count != 1 ||
        Math.Abs(agedWorkers[0].hashrate_hps - expectedAgedHps) / expectedAgedHps > 1e-12)
        throw new InvalidOperationException("expired hashrate boundary bucket did not age out");

    metrics.RemoveSession(identity.SessionId);
    if (metrics.GetWorkers().Count != 0)
        throw new InvalidOperationException("disconnected worker remained in the dashboard snapshot");

    var oneMillisecond = Stopwatch.Frequency / 1000;
    metrics.RecordShareValidation(oneMillisecond);
    metrics.RecordShareValidation(oneMillisecond * 3);
    if (metrics.ShareValidationSamples != 2 ||
        Math.Abs(metrics.ShareValidationAverageMs - 2) > 0.01 ||
        Math.Abs(metrics.ShareValidationMaxMs - 3) > 0.01)
        throw new InvalidOperationException("share validation latency aggregation changed");
}

static void RunMerkleRootCacheChecks()
{
    var branches = Enumerable.Range(0, 5)
        .Select(i => Enumerable.Range(0, 32).Select(j => (byte)(i * 31 + j)).ToArray())
        .ToList();
    var job = new JobTemplate
    {
        Ready = true,
        JobId = "merkle-cache-a",
        Version = 0x20000000,
        Nbits = 0x1d00ffff,
        Ntime = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        PrevhashLe = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray(),
        TargetLe = new byte[32],
        Coinbase2 = Enumerable.Range(0, 32).Select(i => (byte)(255 - i)).ToArray(),
        MerkleBranchesLe = branches,
        Transactions = TransactionSet.Empty
    };
    var prefix = Enumerable.Range(0, 48).Select(i => (byte)i).ToArray();
    var en2a = new byte[] { 0, 0, 0, 7 };
    var en2b = new byte[] { 0, 0, 0, 8 };
    var target = Enumerable.Repeat((byte)0xff, 32).ToArray();
    Span<byte> rootA = stackalloc byte[32];
    Span<byte> rootB = stackalloc byte[32];
    ShareValidator.ComputeMerkleRoot(job, prefix, en2a, rootA);
    ShareValidator.ComputeMerkleRoot(job, prefix, en2b, rootB);
    if (rootA.SequenceEqual(rootB))
        throw new InvalidOperationException("merkle root did not change with extranonce2");

    var direct = ShareValidator.Validate(
        job, prefix, en2a, job.Ntime, nonce: 123, job.Version, target);
    var cached = ShareValidator.ValidateWithMerkleRoot(
        job, prefix, en2a, rootA, job.Ntime, nonce: 123, job.Version, target);
    if (direct.Accepted != cached.Accepted ||
        direct.IsBlock != cached.IsBlock ||
        direct.Hash != cached.Hash ||
        direct.BlockHex != cached.BlockHex ||
        direct.ActualDiff != cached.ActualDiff)
        throw new InvalidOperationException("cached merkle validation changed the share result");

    var malformedVersion = ShareValidator.Validate(
        job,
        prefix,
        new ShareSubmit
        {
            Extranonce2 = Hex.Encode(en2a),
            Ntime = Hex.U32BeHex(job.Ntime),
            Nonce = Hex.U32BeHex(123),
            Version = "not-hex"
        },
        target);
    if (malformedVersion.Accepted || malformedVersion.IsBlock || malformedVersion.Hash != default)
        throw new InvalidOperationException("direct validator ignored a malformed submitted version");

    var cache = new MerkleRootCache();
    cache.Set(job.JobId, en2a, rootA);
    Span<byte> hit = stackalloc byte[32];
    if (!cache.TryGet(job.JobId, en2a, hit) || !hit.SequenceEqual(rootA))
        throw new InvalidOperationException("merkle cache missed the stored tuple");
    if (cache.TryGet(job.JobId, en2b, hit) || cache.TryGet("merkle-cache-b", en2a, hit))
        throw new InvalidOperationException("merkle cache ignored a key component");
    cache.Reset();
    if (cache.TryGet(job.JobId, en2a, hit))
        throw new InvalidOperationException("merkle cache did not reset");

    ShareValidator.ComputeMerkleRoot(job, prefix, en2a, hit);
    ShareValidator.ValidateWithMerkleRoot(
        job, prefix, en2a, hit, job.Ntime, 0, job.Version, job.TargetLe);
    var allocationBefore = GC.GetAllocatedBytesForCurrentThread();
    Span<byte> computed = stackalloc byte[32];
    for (uint nonce = 1; nonce <= 10_000; nonce++)
    {
        ShareValidator.ComputeMerkleRoot(job, prefix, en2a, computed);
        var rejected = ShareValidator.ValidateWithMerkleRoot(
            job, prefix, en2a, computed, job.Ntime, nonce, job.Version, job.TargetLe);
        if (rejected.Accepted || rejected.IsBlock || !rejected.HashComputed)
            throw new InvalidOperationException("zero-target allocation check unexpectedly accepted a share");
    }
    var validationAllocated = GC.GetAllocatedBytesForCurrentThread() - allocationBefore;
    if (validationAllocated != 0)
        throw new InvalidOperationException(
            $"low-difficulty validation/Merkle miss allocated {validationAllocated} bytes");

    cache.Set(job.JobId, en2a, hit);
    allocationBefore = GC.GetAllocatedBytesForCurrentThread();
    Span<byte> cachedRoot = stackalloc byte[32];
    for (var i = 0; i < 10_000; i++)
    {
        if (!cache.TryGet(job.JobId, en2a, cachedRoot))
            throw new InvalidOperationException("allocation check lost the Merkle cache entry");
    }
    var cacheAllocated = GC.GetAllocatedBytesForCurrentThread() - allocationBefore;
    if (cacheAllocated != 0)
        throw new InvalidOperationException($"Merkle cache hit allocated {cacheAllocated} bytes");
}

static void RunAcceptedShareTrackingIntegrationCheck()
{
    var job = new JobTemplate
    {
        Ready = true,
        JobId = "accepted-share-integration",
        Version = 1,
        Nbits = 0x1d00ffff,
        Ntime = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        PrevhashLe = new byte[32],
        TargetLe = new byte[32],
        Coinbase2 = new byte[] { 1, 2, 3 },
        MerkleBranchesLe = new List<byte[]>(),
        Transactions = TransactionSet.Empty
    };
    var result = ShareValidator.Validate(
        job,
        new byte[] { 4, 5, 6 },
        new byte[] { 0, 0, 0, 1 },
        job.Ntime,
        nonce: 0,
        job.Version,
        Enumerable.Repeat((byte)0xff, 32).ToArray());

    if (!result.Accepted || result.IsBlock || result.Hash == default)
        throw new InvalidOperationException(
            $"ordinary accepted share did not expose its value-type header hash: " +
            $"accepted={result.Accepted} block={result.IsBlock} hash={result.Hash}");

    var tracker = new AcceptedShareTracker();
    if (tracker.TryRegister(1, result.Hash) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(1, result.Hash) != AcceptedShareRegistration.Duplicate ||
        tracker.TryRegister(2, result.Hash) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("ordinary accepted share could not be duplicate-tracked");
}

static void RunDifficultyBoundaryChecks()
{
    var target79 = BitcoinEncoding.DiffToShareTargetLe(7.9e16);
    var target80 = BitcoinEncoding.DiffToShareTargetLe(8.0e16);
    var targetMax = BitcoinEncoding.DiffToShareTargetLe(BitcoinEncoding.MaxSupportedShareDifficulty);
    var value79 = new System.Numerics.BigInteger(target79, isUnsigned: true, isBigEndian: false);
    var value80 = new System.Numerics.BigInteger(target80, isUnsigned: true, isBigEndian: false);
    var valueMax = new System.Numerics.BigInteger(targetMax, isUnsigned: true, isBigEndian: false);
    if (value79 <= 0 || value80 <= 0 || valueMax <= 0 || value80 >= value79 || valueMax >= value80)
        throw new InvalidOperationException("high-difficulty targets are not positive and strictly decreasing");

    var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-config-check"));
    cfg.Difficulty.Max = BitcoinEncoding.MaxSupportedShareDifficulty * 2;
    try
    {
        cfg.Validate();
        throw new InvalidOperationException("difficulty.max above the supported ceiling was accepted");
    }
    catch (InvalidOperationException ex) when (ex.Message == "invalid difficulty bounds")
    {
        // expected
    }
}

static void RunExtranonceLeaseChecks()
{
    var pool = new ExtranonceLeasePool(1);
    var leased = new HashSet<uint>();
    for (var i = 0; i < 256; i++)
    {
        if (!pool.TryAcquire(out var value) || !leased.Add(value))
            throw new InvalidOperationException("active extranonce1 leases collided");
        if (StratumServer.EncodeExtranonce1(value, 1).Length != 1)
            throw new InvalidOperationException("extranonce1 encoding size changed");
    }
    if (pool.TryAcquire(out _))
        throw new InvalidOperationException("extranonce1 pool exceeded its one-byte keyspace");

    var released = leased.First();
    if (!pool.Release(released) || !pool.TryAcquire(out var replacement) ||
        replacement != released)
    {
        throw new InvalidOperationException("released extranonce1 lease was not reusable");
    }

    var capacityConfig = OfflineConfig(Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-extranonce-capacity"));
    capacityConfig.Stratum.Extranonce1Size = 1;
    capacityConfig.Stratum.MaxConnections = 256;
    capacityConfig.Validate();
    capacityConfig.Stratum.MaxConnections = 257;
    ExpectException<InvalidOperationException>(
        capacityConfig.Validate,
        "fit the extranonce1 keyspace",
        "stratum connections above extranonce1 capacity");

    Console.WriteLine("PASS extranonce1 lease checks");
}

static void RunWriterWakeSignalShutdownChecks()
{
    for (var i = 0; i < 256; i++)
    {
        var signal = new WriterWakeSignal();
        using var start = new ManualResetEventSlim();
        var producer = Task.Run(() =>
        {
            start.Wait();
            signal.Signal();
        });
        start.Set();
        signal.Dispose();
        producer.GetAwaiter().GetResult();
        signal.Signal();
        signal.Dispose();
    }

    Console.WriteLine("PASS writer wake signal shutdown checks");
}

static void RunNetworkDifficultyClampChecks()
{
    const uint nbits = 0x207fffff;
    var networkTarget = BitcoinEncoding.CompactTargetToLe(nbits);
    var networkDifficulty = BitcoinEncoding.HashToDisplayDiff(networkTarget);
    var config = new DifficultyConfig
    {
        Min = 1,
        Max = BitcoinEncoding.MaxSupportedShareDifficulty,
        Default = 1024
    };
    var job = new JobTemplate
    {
        Ready = true,
        TargetLe = networkTarget,
        NetworkDifficulty = networkDifficulty
    };

    var clamped = StratumServer.ClampDifficultyForJob(config, config.Default, job);
    if (!(clamped > 0 && clamped < networkDifficulty))
        throw new InvalidOperationException("share difficulty was not clamped below network difficulty");
    var shareTarget = BitcoinEncoding.DiffToShareTargetLe(clamped);
    if (!BitcoinEncoding.LeqLe256(networkTarget, shareTarget))
        throw new InvalidOperationException("clamped share target is harder than the network target");

    var configuredTarget = BitcoinEncoding.DiffToShareTargetLe(config.Default);
    if (!StratumServer.RequiresDownwardDifficultyClampForJob(
            config, config.Default, configuredTarget, job) ||
        StratumServer.RequiresDownwardDifficultyClampForJob(
            config, clamped, shareTarget, job) ||
        StratumServer.RequiresDownwardDifficultyClampForJob(
            config, config.Default, configuredTarget, JobTemplate.Empty()))
        throw new InvalidOperationException("network difficulty clamp fast-path changed");
}

static void RunDuplicateShareChecks()
{
    var tracker = new AcceptedShareTracker();
    var prev = new byte[32];
    var merkle = new byte[32];
    var hashV1 = Hash256.FromLittleEndian(BitcoinEncoding.DoubleSha256(
        BitcoinEncoding.BuildHeader(1, prev, merkle, 1, 0x207fffff, 1)));
    var hashV2 = Hash256.FromLittleEndian(BitcoinEncoding.DoubleSha256(
        BitcoinEncoding.BuildHeader(2, prev, merkle, 1, 0x207fffff, 1)));

    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Duplicate ||
        tracker.TryRegister(10, hashV2) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not distinguish effective header versions");

    tracker.Reset();
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not reset for a clean job");

    tracker.Remove(10, hashV1);
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not allow retry after enqueue rollback");

    tracker.TryRegister(11, hashV2);
    tracker.PruneBefore(11);
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(11, hashV2) != AcceptedShareRegistration.Duplicate)
        throw new InvalidOperationException("duplicate tracking did not prune by job epoch");

    var capacityTracker = new AcceptedShareTracker();
    for (var i = 0; i < AcceptedShareTracker.Capacity; i++)
    {
        if (capacityTracker.TryRegister(20, new Hash256((ulong)i, 1, 2, 3)) != AcceptedShareRegistration.Added)
            throw new InvalidOperationException("duplicate tracking reached capacity early");
    }

    var blockHash = new Hash256(ulong.MaxValue, ulong.MaxValue, ulong.MaxValue, ulong.MaxValue);
    if (capacityTracker.TryRegister(20, new Hash256(4, 5, 6, 7)) !=
        AcceptedShareRegistration.CapacityExceeded)
        throw new InvalidOperationException("ordinary share bypassed duplicate tracking capacity");
    if (capacityTracker.TryRegister(20, blockHash, isBlockCandidate: true) !=
            AcceptedShareRegistration.Added ||
        capacityTracker.TryRegister(20, blockHash, isBlockCandidate: true) !=
            AcceptedShareRegistration.Duplicate)
        throw new InvalidOperationException("network block candidate was blocked or not deduplicated at capacity");
}

static async Task RunServiceSupervisorChecksAsync()
{
    using var liveCts = new CancellationTokenSource();
    var socketFailure = new SocketException((int)SocketError.AddressAlreadyInUse);
    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.FromException(socketFailure) }, liveCts.Token);
        throw new InvalidOperationException("service supervisor swallowed a network failure");
    }
    catch (SocketException ex) when (ex.SocketErrorCode == SocketError.AddressAlreadyInUse)
    {
        // expected
    }

    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.CompletedTask }, liveCts.Token);
        throw new InvalidOperationException("service supervisor accepted an unexpected clean stop");
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("stopped unexpectedly", StringComparison.Ordinal))
    {
        // expected
    }

    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.FromCanceled(new CancellationToken(canceled: true)) }, liveCts.Token);
        throw new InvalidOperationException("service supervisor swallowed an unexpected task cancellation");
    }
    catch (OperationCanceledException)
    {
        // expected: the process entry point only treats cancellation as shutdown
        // when its own shutdown token has been canceled.
    }
}

static async Task RunRefreshFailureReadinessCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-readiness-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var metrics = new MetricsStore { LastRefreshOk = true, LastRefreshMs = 123 };
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        var engine = new TemplateEngine(cfg, rpc, metrics, queue);
        try
        {
            await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
            throw new InvalidOperationException("unreachable RPC unexpectedly refreshed a template");
        }
        catch (Exception ex) when (ex is HttpRequestException or TimeoutException)
        {
            // expected
        }

        if (metrics.LastRefreshOk)
            throw new InvalidOperationException("failed template refresh left readiness marked healthy");
        if (metrics.LastRefreshMs != 123)
            throw new InvalidOperationException("failed template refresh rewrote the last-success timestamp");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunBlockOwnershipBeforeResponseCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-ack-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        await queue.StartAsync(CancellationToken.None);

        var candidate = CreateQueueTestBlock(7);
        await queue.EnqueueFoundBlockAsync(candidate.BlockHex, candidate.Hash, 900_001);
        try
        {
            throw new IOException("simulated client response queue failure");
        }
        catch (IOException)
        {
            // The response failure occurs after ownership transferred to the queue.
        }

        await queue.StopAsync(TimeSpan.FromMilliseconds(50));
        if (Directory.GetFiles(queue.PendingDir, "*.json").Length != 1)
            throw new InvalidOperationException("response failure lost the already-enqueued block");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunCleanJobDispatchOrderingCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-clean-order-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.LateShareGraceMs = 0;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        void Publish(JobTemplate job, bool clean) =>
            publish.Invoke(engine, new object[] { job, clean, false });

        async Task DispatchOneAsync(Action<JobNotify> inspect)
        {
            using var dispatchCts = new CancellationTokenSource();
            try
            {
                await engine.DispatchNotificationsAsync(notify =>
                {
                    inspect(notify);
                    dispatchCts.Cancel();
                }, dispatchCts.Token);
            }
            catch (OperationCanceledException) when (dispatchCts.IsCancellationRequested)
            {
                // The requested notification was dispatched and its cleanup completed.
            }
        }

        var oldJob = new JobTemplate { Ready = true, JobId = "clean-order-old" };
        Publish(oldJob, clean: false);
        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, oldJob) || notify.CleanJobs)
                throw new InvalidOperationException("initial non-clean job notification changed");
        });

        var replacement = new JobTemplate { Ready = true, JobId = "clean-order-new" };
        Publish(replacement, clean: true);
        if (engine.FindJob(oldJob.JobId).Job == null || engine.FindJob(replacement.JobId).Job == null)
            throw new InvalidOperationException("clean publish removed old work before notification dispatch");

        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, replacement) || !notify.CleanJobs)
                throw new InvalidOperationException("replacement clean notification changed");
            if (engine.FindJob(oldJob.JobId).Job == null)
                throw new InvalidOperationException("old work disappeared before clean notification was dispatched");
        });

        if (engine.FindJob(oldJob.JobId).Status != JobLookupStatus.RetiredWithinGrace)
            throw new InvalidOperationException("old work was not retained pending clean write completion");
        engine.MarkCleanBroadcastComplete(replacement.Epoch, DateTimeOffset.UtcNow);
        if (engine.FindJob(oldJob.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(replacement.JobId).Job == null)
            throw new InvalidOperationException("old work was not reclaimed after clean write completion");

        // If a same-tip update supersedes a pending clean notification, the merged
        // notification must carry clean=true and cleanup must retain only the latest job.
        var superseding = new JobTemplate { Ready = true, JobId = "clean-order-latest" };
        var coalesced = new JobTemplate { Ready = true, JobId = "clean-order-coalesced" };
        Publish(superseding, clean: true);
        Publish(coalesced, clean: false);
        if (engine.FindJob(replacement.JobId).Job == null || engine.FindJob(superseding.JobId).Job == null)
            throw new InvalidOperationException("coalesced clean publish removed jobs before dispatch");

        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, coalesced) || !notify.CleanJobs)
                throw new InvalidOperationException("pending clean flag was lost during notification coalescing");
            if (engine.FindJob(replacement.JobId).Job == null || engine.FindJob(superseding.JobId).Job == null)
                throw new InvalidOperationException("coalesced old work disappeared before dispatch");
        });

        engine.MarkCleanBroadcastComplete(coalesced.Epoch, DateTimeOffset.UtcNow);
        if (engine.FindJob(replacement.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(superseding.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(coalesced.JobId).Job == null)
            throw new InvalidOperationException("coalesced clean cleanup retained the wrong jobs");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunDirectGbtDoesNotBlockFastPublishCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-gbt-lock-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    var requestReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var releaseResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        var context = await listener.GetContextAsync();
        requestReceived.TrySetResult();
        await releaseResponse.Task;
        var response = Encoding.UTF8.GetBytes(
            "{\"result\":null,\"error\":{\"code\":-1,\"message\":\"offline stop\"},\"id\":\"1\"}");
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var refresh = engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
        await requestReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        if (!engine.TryEnterPublicationForTest())
            throw new InvalidOperationException("direct GBT held the P2P publication lock during RPC I/O");
        engine.ExitPublicationForTest();

        releaseResponse.TrySetResult();
        try
        {
            await refresh.WaitAsync(TimeSpan.FromSeconds(2));
            throw new InvalidOperationException("offline GBT error response unexpectedly succeeded");
        }
        catch (InvalidOperationException)
        {
            // Expected: only semaphore ownership is under test.
        }
        await serverTask.WaitAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        releaseResponse.TrySetResult();
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static void RunGbtResponseOrderingChecks()
{
    var tipA = new string('a', 64);
    var tipB = new string('b', 64);
    var authoritative = new JobTemplate
    {
        Ready = true,
        Height = 100,
        PrevhashBe = tipA
    };

    GbtResponse Response(uint height, string tip, string? longpollId) => new()
    {
        Height = height,
        PreviousBlockhash = tip,
        LongPollId = longpollId
    };

    if (!TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "10"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "11"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipB, tipB + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(99, tipA, tipA + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, "opaque-token"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "9"), JobTemplate.Empty(), tipA + "10"))
        throw new InvalidOperationException("GBT longpoll revision ordering changed");

    var parsed = TemplateEngine.ParseGbtLongpollRevision(tipA + "123");
    if (!parsed.HasValue || parsed.Value.TipHash != tipA ||
        parsed.Value.TransactionsUpdated != 123 ||
        TemplateEngine.ParseGbtLongpollRevision("opaque-token").HasValue)
        throw new InvalidOperationException("GBT longpoll revision parsing changed");

    GbtResponse ExactResponse() => new()
    {
        Version = 0x20000000,
        PreviousBlockhash = tipA,
        CoinbaseValue = 5_000_000_000,
        Target = "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        CurTime = 1_700_000_000,
        Bits = "1d00ffff",
        Height = 100,
        Transactions = Array.Empty<GbtTx>(),
        CoinbaseAux = new GbtCoinbaseAux { Flags = "01aa" },
        DefaultWitnessCommitment = "6a24aa21a9ed" + new string('0', 64),
        LongPollId = tipA + "77",
        Mintime = 1_699_999_999,
        Vbrequired = 0,
        SubmitOld = true
    };

    var exact = ExactResponse();
    var exactIdentity = GbtScalarIdentity.FromResponse(exact);
    bool Matches(GbtResponse candidate) => TemplateEngine.IsExactAppliedGbtResponse(
        candidate, exact.LongPollId, exactIdentity);
    if (!Matches(ExactResponse()))
        throw new InvalidOperationException("identical GBT revision/scalars did not take the O(1) path");

    var changedCurtime = ExactResponse();
    changedCurtime.CurTime++;
    var changedTarget = ExactResponse();
    changedTarget.Target = new string('1', 64);
    var changedCoinbase = ExactResponse();
    changedCoinbase.CoinbaseValue--;
    var changedTransactions = ExactResponse();
    changedTransactions.Transactions = [new GbtTx()];
    var changedRevision = ExactResponse();
    changedRevision.LongPollId = tipA + "78";
    if (Matches(changedCurtime) || Matches(changedTarget) || Matches(changedCoinbase) ||
        Matches(changedTransactions) || Matches(changedRevision))
        throw new InvalidOperationException("GBT O(1) identity ignored a consensus/template scalar");
}

static async Task RunGbtRestartGenerationInvalidationCheckAsync()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-gbt-restart-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    using var serverCts = new CancellationTokenSource();
    var firstLongpollReceived = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    var abortOldLongpoll = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    var restarted = 0;
    var directGbtCount = 0;
    var longpollCount = 0;
    var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var tip = new string('0', 64);
    var target = Hex.Encode(Hex.ReverseCopy(BitcoinEncoding.CompactTargetToLe(0x207fffff)));

    Dictionary<string, object?> GbtResult(bool afterRestart) => new()
    {
        ["version"] = 0x20000000u,
        ["previousblockhash"] = tip,
        ["coinbasevalue"] = 5_000_000_000L,
        ["target"] = target,
        ["curtime"] = now,
        ["bits"] = "207fffff",
        ["height"] = 1u,
        ["transactions"] = new object[]
        {
            new Dictionary<string, object?>
            {
                ["data"] = afterRestart ? "02" : "01",
                ["txid"] = new string(afterRestart ? '2' : '1', 64),
                ["hash"] = new string(afterRestart ? '2' : '1', 64)
            }
        },
        ["coinbaseaux"] = new Dictionary<string, object?> { ["flags"] = "" },
        ["longpollid"] = tip + "7",
        ["mintime"] = now > 0 ? now - 1 : 0,
        ["vbrequired"] = 0u,
        ["submitold"] = true
    };

    static async Task RespondAsync(HttpListenerContext context, object? result)
    {
        var response = JsonSerializer.SerializeToUtf8Bytes(
            new { result, error = (object?)null, id = "1" });
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    }

    var serverTask = Task.Run(async () =>
    {
        try
        {
            while (!serverCts.IsCancellationRequested)
            {
                var context = await listener.GetContextAsync();
                using var request = await JsonDocument.ParseAsync(context.Request.InputStream);
                if (request.RootElement.GetProperty("method").GetString() == "getnetworkhashps")
                {
                    await RespondAsync(context, 0d);
                    continue;
                }

                var parameters = request.RootElement.GetProperty("params");
                var isLongpoll = parameters.GetArrayLength() > 0 &&
                    parameters[0].ValueKind == JsonValueKind.Object &&
                    parameters[0].TryGetProperty("longpollid", out _);
                if (isLongpoll)
                {
                    var attempt = Interlocked.Increment(ref longpollCount);
                    if (attempt == 1)
                    {
                        firstLongpollReceived.TrySetResult();
                        _ = Task.Run(async () =>
                        {
                            await abortOldLongpoll.Task;
                            try { context.Response.Abort(); } catch { }
                        });
                    }
                    continue;
                }

                Interlocked.Increment(ref directGbtCount);
                await RespondAsync(context, GbtResult(Volatile.Read(ref restarted) != 0));
            }
        }
        catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
        {
            // Listener stopped by test cleanup.
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(
            cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        using var engineCts = new CancellationTokenSource();
        await engine.StartAsync(engineCts.Token);
        await firstLongpollReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var originalKey = engine.ActiveMiningJob.TemplateKey;
        Volatile.Write(ref restarted, 1);
        await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, engineCts.Token)
            .WaitAsync(TimeSpan.FromSeconds(2));
        if (engine.ActiveMiningJob.TemplateKey != originalKey ||
            engine.ActiveMiningJob.Transactions.GetTransaction(0)[0] != 0x01)
        {
            throw new InvalidOperationException(
                "reused GBT revision unexpectedly bypassed the generation boundary test");
        }

        abortOldLongpoll.TrySetResult();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (engine.ActiveMiningJob.TemplateKey == originalKey && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        if (engine.ActiveMiningJob.TemplateKey == originalKey ||
            engine.ActiveMiningJob.Transactions.GetTransaction(0)[0] != 0x02 ||
            Volatile.Read(ref directGbtCount) < 3)
        {
            throw new InvalidOperationException(
                "Core restart did not invalidate the reused GBT revision and publish the new transaction set");
        }

        engineCts.Cancel();
        Console.WriteLine("PASS GBT node-generation invalidation check");
    }
    finally
    {
        abortOldLongpoll.TrySetResult();
        serverCts.Cancel();
        listener.Stop();
        try { await serverTask.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunDirectGbtBurstCoalescingCheckAsync()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-gbt-burst-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    using var serverCts = new CancellationTokenSource();
    var firstRequestReceived = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    var releaseFirstResponse = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    var gbtRequestCount = 0;
    var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var tip = new string('0', 64);
    var target = Hex.Encode(Hex.ReverseCopy(BitcoinEncoding.CompactTargetToLe(0x207fffff)));

    Dictionary<string, object?> GbtResult(int attempt) => new()
    {
        ["version"] = 0x20000000u,
        ["previousblockhash"] = tip,
        ["coinbasevalue"] = 5_000_000_000L,
        ["target"] = target,
        ["curtime"] = now + (uint)Math.Max(0, attempt - 1),
        ["bits"] = "207fffff",
        ["height"] = 1u,
        ["transactions"] = Array.Empty<object>(),
        ["coinbaseaux"] = new Dictionary<string, object?> { ["flags"] = "" },
        ["longpollid"] = tip + attempt.ToString(
            System.Globalization.CultureInfo.InvariantCulture),
        ["mintime"] = now > 0 ? now - 1 : 0,
        ["vbrequired"] = 0u,
        ["submitold"] = true
    };

    static async Task RespondAsync(HttpListenerContext context, object? result)
    {
        var response = JsonSerializer.SerializeToUtf8Bytes(
            new { result, error = (object?)null, id = "1" });
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    }

    var serverTask = Task.Run(async () =>
    {
        try
        {
            while (!serverCts.IsCancellationRequested)
            {
                var context = await listener.GetContextAsync();
                using var request = await JsonDocument.ParseAsync(context.Request.InputStream);
                var method = request.RootElement.GetProperty("method").GetString();
                if (method == "getnetworkhashps")
                {
                    await RespondAsync(context, 0d);
                    continue;
                }

                var attempt = Interlocked.Increment(ref gbtRequestCount);
                if (attempt == 1)
                {
                    firstRequestReceived.TrySetResult();
                    await releaseFirstResponse.Task;
                }
                await RespondAsync(context, GbtResult(attempt));
            }
        }
        catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
        {
            // Listener stopped by test cleanup.
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(
            cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);

        using var firstCallerCts = new CancellationTokenSource();
        var first = engine.RefreshDirectAsync(TemplateSource.Startup, firstCallerCts.Token);
        await firstRequestReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        const int concurrentCallers = 98;
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registered = 0;
        var burst = Enumerable.Range(0, concurrentCallers)
            .Select(index => Task.Run(async () =>
            {
                await startGate.Task;
                var source = (index & 1) == 0
                    ? TemplateSource.ZmqHashblock
                    : TemplateSource.ZmqRawblock;
                var refresh = engine.RefreshDirectAsync(source, CancellationToken.None);
                Interlocked.Increment(ref registered);
                await refresh;
            }))
            .ToArray();
        startGate.TrySetResult();

        var registrationDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (Volatile.Read(ref registered) != concurrentCallers &&
               DateTime.UtcNow < registrationDeadline)
            await Task.Delay(1);
        if (Volatile.Read(ref registered) != concurrentCallers)
            throw new TimeoutException("direct GBT burst callers did not register in time");

        // Register this after the concurrent callers so the trailing request must use it.
        var latest = engine.RefreshDirectAsync(TemplateSource.PostSubmit, CancellationToken.None);
        firstCallerCts.Cancel();
        try
        {
            await first;
            throw new InvalidOperationException("canceled direct GBT caller unexpectedly completed");
        }
        catch (OperationCanceledException) when (firstCallerCts.IsCancellationRequested)
        {
            // Caller cancellation must not cancel the shared refresh needed by other callers.
        }
        releaseFirstResponse.TrySetResult();
        await Task.WhenAll(burst.Append(latest))
            .WaitAsync(TimeSpan.FromSeconds(5));

        if (Volatile.Read(ref gbtRequestCount) != 2)
            throw new InvalidOperationException(
                $"100 direct GBT triggers produced {gbtRequestCount} RPCs instead of current+trailing");
        var active = engine.ActiveMiningJob;
        if (!active.Ready || active.Source != TemplateSource.PostSubmit || active.Ntime != now + 1)
            throw new InvalidOperationException("direct GBT trailing refresh did not publish the latest request");
    }
    finally
    {
        releaseFirstResponse.TrySetResult();
        serverCts.Cancel();
        listener.Stop();
        try { await serverTask.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunLongpollDoesNotBlockDirectGbtCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-gbt-race-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    using var testCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var longpollReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var releaseLongpoll = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    Task? longpollResponseTask = null;
    var directGbtCount = 0;

    var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var target = Hex.Encode(Hex.ReverseCopy(BitcoinEncoding.CompactTargetToLe(0x207fffff)));
    var gbtResult = new Dictionary<string, object?>
    {
        ["version"] = 0x20000000u,
        ["previousblockhash"] = new string('0', 64),
        ["coinbasevalue"] = 5_000_000_000L,
        ["target"] = target,
        ["curtime"] = now,
        ["bits"] = "207fffff",
        ["height"] = 1u,
        ["transactions"] = Array.Empty<object>(),
        ["coinbaseaux"] = new Dictionary<string, object?> { ["flags"] = "" },
        ["longpollid"] = "race-longpoll",
        ["mintime"] = now > 0 ? now - 1 : 0,
        ["vbrequired"] = 0u,
        ["submitold"] = true
    };

    static async Task RespondAsync(HttpListenerContext context, object? result)
    {
        var response = JsonSerializer.SerializeToUtf8Bytes(new { result, error = (object?)null, id = "1" });
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    }

    var serverTask = Task.Run(async () =>
    {
        try
        {
            while (!testCts.IsCancellationRequested)
            {
                var context = await listener.GetContextAsync();
                string body;
                using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                    body = await reader.ReadToEndAsync();
                using var request = JsonDocument.Parse(body);
                var method = request.RootElement.GetProperty("method").GetString();
                if (method == "getnetworkhashps")
                {
                    await RespondAsync(context, 0d);
                    continue;
                }

                var parameters = request.RootElement.GetProperty("params");
                var isLongpoll = parameters.GetArrayLength() > 0 &&
                    parameters[0].ValueKind == JsonValueKind.Object &&
                    parameters[0].TryGetProperty("longpollid", out _);
                if (isLongpoll)
                {
                    longpollReceived.TrySetResult();
                    longpollResponseTask = Task.Run(async () =>
                    {
                        await releaseLongpoll.Task;
                        try { await RespondAsync(context, gbtResult); }
                        catch { context.Response.Abort(); }
                    });
                    continue;
                }

                Interlocked.Increment(ref directGbtCount);
                await RespondAsync(context, gbtResult);
            }
        }
        catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
        {
            // listener stopped by test cleanup
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        using var engineCts = new CancellationTokenSource();
        await engine.StartAsync(engineCts.Token);
        await longpollReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, engineCts.Token)
            .WaitAsync(TimeSpan.FromSeconds(2));
        if (Volatile.Read(ref directGbtCount) < 2)
            throw new InvalidOperationException("direct GBT did not run while longpoll was outstanding");

        engineCts.Cancel();
        releaseLongpoll.TrySetResult();
        if (longpollResponseTask != null)
            await longpollResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        releaseLongpoll.TrySetResult();
        testCts.Cancel();
        listener.Stop();
        try { await serverTask.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static void RunLatestJobQueueCoalescingCheck()
{
    long nextSequence = 40;
    var dirtyA = new byte[] { 1 };
    var cleanA = new byte[] { 2 };
    var difficultyA = new byte[] { 7 };
    var first = JobOutboundFrame.ReplacePending(
        null,
        () => ++nextSequence,
        lastQueuedDifficultySequence: 0,
        epoch: 100,
        cleanJobs: true,
        versionFrame: null,
        notifyFrame: dirtyA,
        cleanNotifyFrame: cleanA,
        difficultyFrame: difficultyA);
    if (!first.CleanJobs || !ReferenceEquals(first.Frame, cleanA) ||
        !ReferenceEquals(first.DifficultyFrame, difficultyA) || first.Sequence != 41)
        throw new InvalidOperationException("first clean job frame was not queued correctly");

    var dirtyB = new byte[] { 3 };
    var cleanB = new byte[] { 4 };
    var replacement = JobOutboundFrame.ReplacePending(
        first,
        () => ++nextSequence,
        lastQueuedDifficultySequence: 0,
        epoch: 101,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyB,
        cleanNotifyFrame: cleanB);
    if (replacement.Epoch != 101 || !replacement.CleanJobs ||
        replacement.Sequence != first.Sequence ||
        !ReferenceEquals(replacement.Frame, cleanB) ||
        !ReferenceEquals(replacement.DifficultyFrame, difficultyA) || nextSequence != 41)
        throw new InvalidOperationException(
            "latest-wins replacement lost clean state or its ordered queue position");

    var difficultyB = new byte[] { 8 };
    var latestDifficulty = JobOutboundFrame.ReplacePending(
        replacement,
        () => ++nextSequence,
        lastQueuedDifficultySequence: 0,
        epoch: 102,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyB,
        cleanNotifyFrame: cleanB,
        difficultyFrame: difficultyB);
    if (latestDifficulty.Sequence != first.Sequence ||
        !ReferenceEquals(latestDifficulty.DifficultyFrame, difficultyB) || nextSequence != 41)
        throw new InvalidOperationException(
            "coalesced public job did not retain the latest pending difficulty");

    var difficultySequence = ++nextSequence;
    var afterDifficulty = JobOutboundFrame.ReplacePending(
        latestDifficulty,
        () => ++nextSequence,
        lastQueuedDifficultySequence: difficultySequence,
        epoch: 103,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyB,
        cleanNotifyFrame: cleanB);
    if (afterDifficulty.Sequence <= difficultySequence ||
        afterDifficulty.DifficultyFrame != null || nextSequence != 43)
        throw new InvalidOperationException(
            "replacement job crossed an intervening difficulty ordering barrier");

    var dirtyC = new byte[] { 5 };
    var cleanC = new byte[] { 6 };
    var independent = JobOutboundFrame.ReplacePending(
        null,
        () => ++nextSequence,
        lastQueuedDifficultySequence: difficultySequence,
        epoch: 104,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyC,
        cleanNotifyFrame: cleanC);
    if (independent.CleanJobs || !ReferenceEquals(independent.Frame, dirtyC) || independent.Sequence != 44)
        throw new InvalidOperationException("independent dirty job frame changed semantics");
}

static void RunRetainedTransactionBudgetCheck()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-retained-budget-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Runtime.MaxRetainedTransactionBytes = 100;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused");
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        var dirtyEngine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var dirtyFirst = new JobTemplate
        {
            Ready = true,
            JobId = "63",
            JobKey = 0x63,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[60] })
        };
        var dirtySecond = new JobTemplate
        {
            Ready = true,
            JobId = "64",
            JobKey = 0x64,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[60] })
        };
        publish.Invoke(dirtyEngine, new object[] { dirtyFirst, false, true });
        publish.Invoke(dirtyEngine, new object[] { dirtySecond, false, true });
        if (dirtyEngine.FindJob(dirtyFirst.JobKey).Status != JobLookupStatus.Expired ||
            dirtyEngine.FindJob(dirtySecond.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException(
                "retained transaction budget did not evict the oldest non-retired job");

        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);

        var first = new JobTemplate
        {
            Ready = true,
            JobId = "65",
            JobKey = 0x65,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[80] })
        };
        var second = new JobTemplate
        {
            Ready = true,
            JobId = "66",
            JobKey = 0x66,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[80] })
        };
        publish.Invoke(engine, new object[] { first, false, true });
        publish.Invoke(engine, new object[] { second, true, true });
        if (engine.FindJob(first.JobKey).Status != JobLookupStatus.Expired ||
            engine.FindJob(second.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("retained transaction budget did not evict the oldest retired job");

        var oversizedActive = new JobTemplate
        {
            Ready = true,
            JobId = "67",
            JobKey = 0x67,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[200] })
        };
        publish.Invoke(engine, new object[] { oversizedActive, true, true });
        if (engine.FindJob(second.JobKey).Status != JobLookupStatus.Expired ||
            engine.FindJob(oversizedActive.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("transaction budget reclaimed the active job or lost its tombstone");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static void RunJobLookupSnapshotPublicationCheck()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-job-lookup-publication-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.LateShareGraceMs = 0;
        cfg.Runtime.RetiredJobMaxAgeSecs = 1;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused");
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        void Publish(JobTemplate job, bool clean) =>
            publish.Invoke(engine, new object[] { job, clean, false });

        var first = new JobTemplate
        {
            Ready = true,
            JobId = "lookup-publication-first",
            JobKey = 0x701
        };
        Publish(first, clean: false);
        var afterFirstPublish = engine.JobLookupSnapshotPublicationCount;
        if (afterFirstPublish == 0 ||
            engine.FindJob(first.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("initial job lookup snapshot was not published");

        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < 8; i++)
            engine.ReclaimRetiredJobs(now);
        if (engine.JobLookupSnapshotPublicationCount != afterFirstPublish)
            throw new InvalidOperationException("no-op job reclaim republished the lookup snapshot");

        var second = new JobTemplate
        {
            Ready = true,
            JobId = "lookup-publication-second",
            JobKey = 0x702
        };
        Publish(second, clean: true);
        var afterCleanPublish = engine.JobLookupSnapshotPublicationCount;
        if (afterCleanPublish != afterFirstPublish + 1 ||
            engine.FindJob(first.JobKey).Status != JobLookupStatus.RetiredWithinGrace ||
            engine.FindJob(second.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("clean job publication did not atomically update lookup state");

        var completedAt = DateTimeOffset.UtcNow;
        engine.MarkCleanBroadcastComplete(second.Epoch, completedAt);
        var afterRetiredReclaim = engine.JobLookupSnapshotPublicationCount;
        if (afterRetiredReclaim != afterCleanPublish + 1 ||
            engine.FindJob(first.JobKey).Status != JobLookupStatus.Expired ||
            engine.FindJob(second.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("retired job reclaim did not publish its lookup tombstone");

        for (var i = 0; i < 8; i++)
            engine.ReclaimRetiredJobs(completedAt);
        if (engine.JobLookupSnapshotPublicationCount != afterRetiredReclaim)
            throw new InvalidOperationException("stable tombstones caused redundant lookup publications");

        var afterTombstoneExpiry = completedAt.AddSeconds(6);
        engine.ReclaimRetiredJobs(afterTombstoneExpiry);
        var afterTombstoneReclaim = engine.JobLookupSnapshotPublicationCount;
        if (afterTombstoneReclaim != afterRetiredReclaim + 1 ||
            engine.FindJob(first.JobKey).Status != JobLookupStatus.Unknown)
            throw new InvalidOperationException("expired lookup tombstone was not reclaimed and published");

        engine.ReclaimRetiredJobs(afterTombstoneExpiry);
        if (engine.JobLookupSnapshotPublicationCount != afterTombstoneReclaim)
            throw new InvalidOperationException("post-expiry no-op reclaim republished the lookup snapshot");

        Console.WriteLine("PASS job lookup snapshot publication checks");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunClientJobWriteProgressCheckAsync()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
    listener.Start();
    var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
    using var miner = new TcpClient();
    var connect = miner.ConnectAsync(System.Net.IPAddress.Loopback, port);
    using var serverTcp = await listener.AcceptTcpClientAsync();
    await connect;
    listener.Stop();

    var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-check"));
    cfg.Stratum.WriteTimeoutSecs = 2;
    cfg.Stratum.SendQueueCapacity = 4;
    var metrics = new MetricsStore();
    using var session = new ClientSession(serverTcp, cfg, metrics);
    var outboundLock = typeof(ClientSession).GetField(
        "_outboundLock", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
        ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession outbound lock was not found");
    var cleanNotify = System.Text.Encoding.UTF8.GetBytes("clean-latest\n");
    var dirtyNotify = System.Text.Encoding.UTF8.GetBytes("dirty-latest\n");
    var difficultyNotify = System.Text.Encoding.UTF8.GetBytes("difficulty\n");

    // A repeated subscription discards its pre-existing pending job, then queues the
    // response before replacement work. Holding the writer lock makes the ordering
    // deterministic instead of depending on loopback scheduling.
    lock (outboundLock)
    {
        if (!session.TryQueueJob(
                epoch: 400,
                cleanJobs: true,
                versionFrame: null,
                notifyFrame: dirtyNotify,
                cleanNotifyFrame: Encoding.UTF8.GetBytes("discarded-job\n")))
            throw new InvalidOperationException("client writer rejected the pre-subscribe job");
        session.DiscardPendingJob();
        if (!session.TryQueueWrite(Encoding.UTF8.GetBytes("subscribe-response\n")))
            throw new InvalidOperationException("client writer rejected the subscribe response");
        if (!session.TryQueueJob(
                epoch: 450,
                cleanJobs: true,
                versionFrame: null,
                notifyFrame: dirtyNotify,
                cleanNotifyFrame: Encoding.UTF8.GetBytes("subscription-job\n")))
            throw new InvalidOperationException("client writer rejected the subscription job");
    }

    using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var expected = "subscribe-response\nsubscription-job\n";
    var buffer = new byte[512];
    var read = 0;
    while (read < expected.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("client writer closed before subscription work");
        read += count;
    }
    var text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expected)
        throw new InvalidOperationException($"subscribe response/job ordering changed: {text}");

    // Reproduce the P2P-fast -> standalone VarDiff -> full GBT replacement race.
    // The full job inherits clean state, but not the fast job's pre-difficulty slot.
    lock (outboundLock)
    {
        if (!session.TryQueueJob(
                epoch: 475,
                cleanJobs: true,
                versionFrame: null,
                notifyFrame: dirtyNotify,
                cleanNotifyFrame: Encoding.UTF8.GetBytes("p2p-fast-job\n")))
            throw new InvalidOperationException("client writer rejected the P2P-fast job");
        if (!session.TryQueueDifficulty(Encoding.UTF8.GetBytes("vardiff\n")))
            throw new InvalidOperationException("client writer rejected the VarDiff frame");
        if (!session.TryQueueJob(
                epoch: 480,
                cleanJobs: false,
                versionFrame: null,
                notifyFrame: Encoding.UTF8.GetBytes("full-gbt-dirty\n"),
                cleanNotifyFrame: Encoding.UTF8.GetBytes("full-gbt-clean\n")))
            throw new InvalidOperationException("client writer rejected the full GBT job");
    }

    expected = "vardiff\nfull-gbt-clean\n";
    read = 0;
    while (read < expected.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("client writer closed during the difficulty barrier check");
        read += count;
    }
    text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expected)
        throw new InvalidOperationException($"difficulty/job ordering changed: {text}");

    if (!session.TryQueueJob(
            epoch: 500,
            cleanJobs: true,
            versionFrame: null,
            notifyFrame: dirtyNotify,
            cleanNotifyFrame: cleanNotify,
            difficultyFrame: difficultyNotify))
        throw new InvalidOperationException("client writer rejected the clean job test frame");

    expected = "difficulty\nclean-latest\n";
    read = 0;
    while (read < expected.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("client writer closed before the complete job frame");
        read += count;
    }
    text = System.Text.Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expected)
        throw new InvalidOperationException($"client writer emitted the wrong job frame: {text}");

    var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (session.LastWrittenJobEpoch < 500 && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (session.LastWrittenJobEpoch != 500)
        throw new InvalidOperationException("job epoch advanced before or failed to advance after socket write");

    var acceptedStarted = Stopwatch.GetTimestamp();
    var pooledBaseline = OutboundFrame.OutstandingPooledBufferCount;
    const string acceptedText = "{\"id\":7,\"result\":true,\"error\":null}\n";
    if (!session.TryQueueAcceptedShareResponse(
            StratumServer.BuildPooledOkTrueFrame(StratumRequestId.FromInt64(7)), acceptedStarted))
        throw new InvalidOperationException("client writer rejected the accepted response test frame");
    if (metrics.AcceptedShareAckQueued != 1 || metrics.AcceptedShareAckWritten > 1)
        throw new InvalidOperationException("accepted response metrics advanced at the wrong queue stage");
    read = await miner.GetStream().ReadAsync(buffer, readCts.Token);
    text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != acceptedText)
        throw new InvalidOperationException($"client writer emitted the wrong accepted frame: {text}");
    deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (metrics.AcceptedShareAckWritten < 1 && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (metrics.AcceptedShareAckWritten != 1 || metrics.AcceptedShareAckWriteMaxMs < 0)
        throw new InvalidOperationException("accepted response socket-write metric did not advance");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("normally written accepted response was not returned to the pool");

    lock (outboundLock)
    {
        for (var i = 0; i < cfg.Stratum.SendQueueCapacity; i++)
        {
            if (!session.TryQueuePooledWrite(StratumServer.BuildPooledStratumErrorFrame(
                    StratumRequestId.FromInt64(9), 23, "Low difficulty share")))
                throw new InvalidOperationException("pooled send queue filled before its configured capacity");
        }
        if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + cfg.Stratum.SendQueueCapacity)
            throw new InvalidOperationException("queued pooled response ownership count changed");
        if (session.TryQueuePooledWrite(StratumServer.BuildPooledStratumErrorFrame(
                StratumRequestId.FromInt64(10), 23, "Low difficulty share")))
            throw new InvalidOperationException("full send queue accepted another pooled response");
        if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + cfg.Stratum.SendQueueCapacity)
            throw new InvalidOperationException("queue-full path did not immediately return its pooled response");
    }

    var oneError = "{\"id\":9,\"result\":false,\"error\":[23,\"Low difficulty share\",null]}\n";
    var expectedErrors = string.Concat(Enumerable.Repeat(oneError, cfg.Stratum.SendQueueCapacity));
    read = 0;
    while (read < expectedErrors.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("writer closed before draining pooled error frames");
        read += count;
    }
    text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expectedErrors)
        throw new InvalidOperationException("pooled queue drain changed response ordering or bytes");
    deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("drained queue retained pooled response buffers");

    await session.StopWriterAsync();
    var closedFrame = StratumServer.BuildPooledStratumErrorFrame(
        StratumRequestId.FromInt64(8), 23, "Low difficulty share");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + 1)
        throw new InvalidOperationException("closed-queue test frame did not acquire pool ownership");
    if (session.TryQueuePooledWrite(closedFrame))
        throw new InvalidOperationException("closed writer accepted a pooled response");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("closed writer did not return the rejected pooled response");
}

static async Task RunPooledWriterFailureChecksAsync()
{
    static async Task<(TcpClient Miner, TcpClient Server)> ConnectPairAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        var miner = new TcpClient();
        var connect = miner.ConnectAsync(endpoint.Address, endpoint.Port);
        var server = await listener.AcceptTcpClientAsync();
        await connect;
        return (miner, server);
    }

    var baseline = OutboundFrame.OutstandingPooledBufferCount;
    var cancelPair = await ConnectPairAsync();
    using (cancelPair.Miner)
    using (cancelPair.Server)
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-cancel"));
        cfg.Stratum.SendQueueCapacity = 4;
        cfg.Stratum.WriteTimeoutSecs = 1;
        using var session = new ClientSession(cancelPair.Server, cfg);
        var gate = typeof(ClientSession).GetField(
            "_outboundLock", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession outbound lock was not found");
        var writerCts = (CancellationTokenSource)(typeof(ClientSession).GetField(
            "_writerCts", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession writer CTS was not found"));
        lock (gate)
        {
            for (var i = 0; i < cfg.Stratum.SendQueueCapacity; i++)
            {
                if (!session.TryQueuePooledWrite(StratumServer.BuildPooledOkTrueFrame(
                        StratumRequestId.FromInt64(i))))
                    throw new InvalidOperationException("cancellation test could not fill the pooled queue");
            }
            writerCts.Cancel();
        }
        await session.StopWriterAsync();
        if (OutboundFrame.OutstandingPooledBufferCount != baseline)
            throw new InvalidOperationException("writer cancellation did not drain pooled response buffers");
    }

    var resetPair = await ConnectPairAsync();
    using (resetPair.Server)
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-reset"));
        cfg.Stratum.WriteTimeoutSecs = 1;
        using var session = new ClientSession(resetPair.Server, cfg);
        resetPair.Miner.Client.LingerState = new LingerOption(enable: true, seconds: 0);
        resetPair.Miner.Close();
        await Task.Delay(25);
        for (var i = 0; i < 4; i++)
        {
            if (!session.TryQueuePooledWrite(StratumServer.BuildPooledOkTrueFrame(
                    StratumRequestId.FromInt64(i))))
                break;
        }
        var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
        while ((!session.WriterUnavailable || OutboundFrame.OutstandingPooledBufferCount != baseline) &&
               DateTimeOffset.UtcNow < deadline)
            await Task.Delay(5);
        await session.StopWriterAsync();
        if (!session.WriterUnavailable || OutboundFrame.OutstandingPooledBufferCount != baseline)
            throw new InvalidOperationException("socket failure did not close the writer and return pooled buffers");
    }
}

static async Task RunCrossSourceCleanLifecycleCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-source-lifecycle-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.LateShareGraceMs = 0;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");
        void Publish(JobTemplate job, bool clean, bool authoritative) =>
            publish.Invoke(engine, new object[] { job, clean, authoritative });

        async Task<JobNotify> DispatchOneAsync()
        {
            JobNotify captured = default;
            using var cts = new CancellationTokenSource();
            try
            {
                await engine.DispatchNotificationsAsync(notify =>
                {
                    captured = notify;
                    cts.Cancel();
                }, cts.Token);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            return captured;
        }

        var longpoll = new JobTemplate
        {
            Ready = true,
            JobId = "source-longpoll",
            Source = TemplateSource.Longpoll,
            Height = 900_001,
            PrevhashBe = new string('a', 64)
        };
        Publish(longpoll, clean: false, authoritative: true);
        await DispatchOneAsync();

        var p2pFast = new JobTemplate
        {
            Ready = true,
            JobId = "source-p2p-fast",
            Source = TemplateSource.P2pFast,
            Height = 900_002,
            PrevhashBe = new string('b', 64)
        };
        Publish(p2pFast, clean: true, authoritative: false);
        if (engine.AuthoritativeJob.Epoch != longpoll.Epoch ||
            engine.TryUseAuthoritativeJob(_ => { }))
            throw new InvalidOperationException(
                "P2P-fast work replaced or exposed the full-GBT authoritative snapshot");

        var zmqFull = new JobTemplate
        {
            Ready = true,
            JobId = "source-zmq-full",
            Source = TemplateSource.ZmqHashblock,
            Height = p2pFast.Height,
            PrevhashBe = p2pFast.PrevhashBe,
            SubmitOld = false
        };
        if (TemplateEngine.ShouldCleanGbtUpdate(p2pFast, zmqFull))
            throw new InvalidOperationException("same-tip P2P-fast confirmation changed clean policy");
        Publish(zmqFull, clean: false, authoritative: true);
        JobTemplate? selectedAuthoritative = null;
        if (!engine.TryUseAuthoritativeJob(job => selectedAuthoritative = job) ||
            !ReferenceEquals(selectedAuthoritative, zmqFull))
            throw new InvalidOperationException("confirmed full GBT was not selected as authoritative work");

        var merged = await DispatchOneAsync();
        if (!ReferenceEquals(merged.Job, zmqFull) || !merged.CleanJobs || merged.Epoch != zmqFull.Epoch)
            throw new InvalidOperationException("P2P-fast clean was lost when ZMQ full GBT superseded it");
        if (engine.FindJob(longpoll.JobId).Status != JobLookupStatus.RetiredWithinGrace ||
            engine.FindJob(p2pFast.JobId).Status != JobLookupStatus.RetiredWithinGrace)
            throw new InvalidOperationException("cross-source old jobs were not retained for late shares");

        var nextLongpoll = new JobTemplate
        {
            Ready = true,
            JobId = "source-next-longpoll",
            Source = TemplateSource.Longpoll,
            Height = 900_003,
            PrevhashBe = new string('c', 64)
        };
        Publish(nextLongpoll, clean: true, authoritative: true);
        var nextNotify = await DispatchOneAsync();
        if (!nextNotify.CleanJobs || nextNotify.Epoch <= merged.Epoch)
            throw new InvalidOperationException("consecutive clean source epochs were not monotonic");

        // A newer actually-written epoch satisfies all earlier clean barriers.
        engine.MarkCleanBroadcastComplete(nextNotify.Epoch, DateTimeOffset.UtcNow);
        foreach (var expiredId in new[] { longpoll.JobId, p2pFast.JobId, zmqFull.JobId })
        {
            if (engine.FindJob(expiredId).Status != JobLookupStatus.Expired)
                throw new InvalidOperationException($"cross-source retired job was not reclaimed: {expiredId}");
        }
        if (engine.FindJob(nextLongpoll.JobId).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("latest longpoll job was reclaimed by an older clean barrier");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunSyntheticGbtStressChecksAsync()
{
    Console.WriteLine("=== synthetic GBT stress validation ===");
    await RunSyntheticGbtStressCaseAsync(10_000);
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    await RunSyntheticGbtStressCaseAsync(20_000);
    Console.WriteLine("PASS synthetic GBT 10k/20k stress checks");
}

static async Task RunSyntheticGbtStressCaseAsync(int transactionCount)
{
    const int rawTransactionBytes = 90;
    var transactions = new GbtTx[transactionCount];
    for (var i = 0; i < transactionCount; i++)
    {
        var seed = BitConverter.GetBytes(i);
        var hash = System.Security.Cryptography.SHA256.HashData(seed);
        var data = new byte[rawTransactionBytes];
        for (var offset = 0; offset < data.Length; offset++)
            data[offset] = hash[offset % hash.Length];
        BitConverter.TryWriteBytes(data.AsSpan(0, sizeof(int)), i);
        var txid = Hex.Encode(hash);
        transactions[i] = new GbtTx
        {
            Data = data,
            TxId = txid,
            Hash = txid
        };
    }

    GbtResponse? source = new()
    {
        Version = 0x20000000,
        PreviousBlockhash = new string('1', 64),
        CoinbaseValue = 5_000_000_000,
        Target = "7fffff" + new string('0', 58),
        CurTime = 1_700_000_000,
        Bits = "207fffff",
        Height = 1_000_000,
        Transactions = transactions,
        CoinbaseAux = new GbtCoinbaseAux { Flags = "062f503253482f" },
        DefaultWitnessCommitment = "6a24aa21a9ed" + new string('0', 64),
        Mintime = 1_700_000_000,
        SubmitOld = true
    };
    var jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
    var json = JsonSerializer.SerializeToUtf8Bytes(source, jsonOptions);
    source = null;
    transactions = null!;

    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var parseTimer = Stopwatch.StartNew();
    using var jsonStream = new MemoryStream(json, writable: false);
    var parsed = await JsonSerializer.DeserializeAsync<GbtResponse>(jsonStream, jsonOptions)
        ?? throw new InvalidOperationException("synthetic GBT deserialized to null");
    parseTimer.Stop();

    var cfg = OfflineConfig(Path.GetTempPath());
    var builder = new JobBuilder(cfg);
    var keyTimer = Stopwatch.StartNew();
    var keyParts = builder.ComputeTemplateKeyParts(parsed);
    keyTimer.Stop();
    var buildTimer = Stopwatch.StartNew();
    var job = builder.FromGbt(
        parsed, TemplateSource.Startup, keyParts.Key, keyParts.TxHashesLe);
    buildTimer.Stop();
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

    var expectedTransactionBytes = (long)transactionCount * rawTransactionBytes;
    var expectedMerkleBranches = (int)Math.Ceiling(Math.Log2(transactionCount + 1));
    if (!job.Ready || job.TransactionCount != transactionCount ||
        job.TransactionBytes != expectedTransactionBytes ||
        keyParts.TxHashesLe.Count != transactionCount ||
        job.MerkleBranchesLe.Count != expectedMerkleBranches)
    {
        throw new InvalidOperationException(
            $"synthetic GBT {transactionCount:N0} invariant failed: " +
            $"job_txs={job.TransactionCount} tx_bytes={job.TransactionBytes} " +
            $"leaves={keyParts.TxHashesLe.Count} branches={job.MerkleBranchesLe.Count}");
    }
    var middle = transactionCount / 2;
    if (parsed.PackedTransactions == null || parsed.TransactionCount != transactionCount ||
        !job.Transactions.GetTransaction(middle).SequenceEqual(
            parsed.PackedTransactions.Transactions.GetTransaction(middle)))
        throw new InvalidOperationException("synthetic GBT job changed transaction payload");

    var coinbasePrefix = new byte[job.Coinbase1.Length + 4];
    job.Coinbase1.CopyTo(coinbasePrefix, 0);
    var merkleRoot = new byte[32];
    ShareValidator.ComputeMerkleRoot(
        job, coinbasePrefix, new byte[4], merkleRoot);
    if (merkleRoot.All(value => value == 0))
        throw new InvalidOperationException("synthetic GBT produced an empty Merkle root");
    if (allocated > 512L * 1024 * 1024)
        throw new InvalidOperationException(
            $"synthetic GBT {transactionCount:N0} allocated {allocated / 1024d / 1024d:F1} MiB");

    Console.WriteLine(
        $"synthetic GBT txs={transactionCount:N0} json={json.Length / 1024d / 1024d:F2} MiB " +
        $"raw={job.TransactionBytes / 1024d / 1024d:F2} MiB branches={job.MerkleBranchesLe.Count} " +
        $"parse={parseTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"fingerprint={keyTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"build={buildTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"managed_alloc={allocated / 1024d / 1024d:F1} MiB");
}

static AppConfig OfflineConfig(string dataDir)
{
    var payout = new Key().PubKey.GetAddress(ScriptPubKeyType.Segwit, Network.RegTest).ToString();
    var cfg = new AppConfig
    {
        NetworkName = "regtest",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = "http://127.0.0.1:1",
            RpcUser = "unused",
            RpcPassword = "unused"
        },
        Coinbase = new CoinbaseConfig { Address = payout },
        Runtime = new RuntimeConfig { DataDir = dataDir },
        Difficulty = new DifficultyConfig { Min = 1, Max = 1e12, Default = 1 }
    };
    cfg.Validate();
    return cfg;
}

static async Task RunExtranonceMatrixAsync(
    BitcoinRpcClient rpc,
    string payout,
    string workDir,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    string stratumHost)
{
    RunExtranonceConfigRangeChecks();

    Console.WriteLine("--- [1/3] DIRECT: JobBuilder + ShareValidator + submitblock for 32 size pairs ---");
    for (var en1Size = 1; en1Size <= 4; en1Size++)
    {
        for (var en2Size = 1; en2Size <= 8; en2Size++)
        {
            await RunDirectExtranonceComboAsync(rpc, payout, rpcUrl, rpcUser, rpcPass, en1Size, en2Size);
        }
    }

    Console.WriteLine("--- [2/3] STRATUM: subscribe size + mining.submit + gateway submitblock for 32 size pairs ---");
    for (var en1Size = 1; en1Size <= 4; en1Size++)
    {
        for (var en2Size = 1; en2Size <= 8; en2Size++)
        {
            await RunStratumExtranonceComboAsync(
                rpc, payout, workDir, rpcUrl, rpcUser, rpcPass, stratumHost, en1Size, en2Size);
        }
    }

    Console.WriteLine("PASS extranonce matrix totals: 32 direct blocks + 32 production-gateway blocks");
    Console.WriteLine("--- [3/3] BOUNDARY CROSS: witness + BIP310 + clean/late/stale ---");
    await RunExtranonceBoundaryCrossAsync(
        rpc, payout, workDir, rpcUrl, rpcUser, rpcPass, stratumHost);
}

static async Task RunExtranonceBoundaryCrossAsync(
    BitcoinRpcClient rpc,
    string payout,
    string workDir,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    string stratumHost)
{
    var boundaryPairs = new (int Extranonce1Size, int Extranonce2Size)[]
    {
        (1, 1), (1, 8), (4, 1), (4, 8)
    };

    foreach (var pair in boundaryPairs)
    {
        var label = $"boundary en1={pair.Extranonce1Size} en2={pair.Extranonce2Size}";
        var comboDir = Path.Combine(
            workDir, $"boundary-en1{pair.Extranonce1Size}-en2{pair.Extranonce2Size}");
        var stratumPort = GetFreeTcpPort();
        var apiPort = GetFreeTcpPort();
        var configPath = await WriteRuntimeConfigAsync(
            comboDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort,
            lifecycleMode: true,
            extranonce1Size: pair.Extranonce1Size,
            extranonce2Size: pair.Extranonce2Size);

        var blockTxids = await SeedMempoolAsync(rpc, count: 3);
        Process? gateway = StartGateway(configPath);
        try
        {
            await WaitHttpAsync(
                $"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));
            await RunStratumPathAsync(
                rpc, stratumHost, stratumPort, apiPort,
                expectedTxids: blockTxids,
                requireAllExpectedTxids: true,
                expectedExtranonce2Size: pair.Extranonce2Size);

            TryKill(gateway);
            gateway = null;

            // Restarting the published artifact forces an immediate startup GBT with
            // the second witness set instead of waiting for Core's mempool longpoll timer.
            var lifecycleTxids = await SeedMempoolAsync(rpc, count: 3);
            gateway = StartGateway(configPath);
            await WaitHttpAsync(
                $"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));
            await RunStratumLifecyclePathAsync(
                rpc, stratumHost, stratumPort, apiPort, payout,
                minerCount: 4,
                extranonce2Size: pair.Extranonce2Size,
                requireInitialMempool: true,
                expectedInitialTxids: lifecycleTxids);

            Console.WriteLine(
                $"PASS {label} witness-block+BIP310+clean/late/stale");
        }
        finally
        {
            TryKill(gateway);
        }
    }
}

static async Task RunOwnedCoreRestartRecoveryAsync(
    Process initialBitcoind,
    Action<Process?> updateOwnedBitcoind,
    BitcoinRpcClient rpc,
    string payout,
    string workDir,
    string datadir,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    string stratumHost)
{
    AssertOwnedBitcoind(initialBitcoind, workDir, datadir, rpcUrl);

    var scenarioDir = Path.Combine(workDir, "core-restart-gateway");
    var stratumPort = GetFreeTcpPort();
    var apiPort = GetFreeTcpPort();
    var configPath = await WriteRuntimeConfigAsync(
        scenarioDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort);
    var gatewayDataDir = Path.Combine(scenarioDir, "gateway-data");
    var pendingDir = Path.Combine(gatewayDataDir, "pending-blocks");
    var failedDir = Path.Combine(gatewayDataDir, "failed-blocks");
    var logs = new ConcurrentQueue<string>();

    Process? currentBitcoind = initialBitcoind;
    Process? gateway = StartGateway(configPath, logs);
    try
    {
        await WaitHttpAsync(
            $"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));

        using var client = new StratumMinerClient();
        await client.ConnectAsync(stratumHost, stratumPort);
        var negotiatedMask = await client.ConfigureVersionRollingAsync("1fffe000");
        var (extranonce1, extranonce2Size) =
            await client.SubscribeAsync("regtest-core-restart/1.0");
        if (extranonce2Size != 4)
            throw new InvalidOperationException($"core-restart extranonce2_size={extranonce2Size}, expected 4");
        await client.AuthorizeAsync("restart-worker", "x");
        await client.WaitForDifficultyAsync(TimeSpan.FromSeconds(5));
        var beforeRestart = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));

        await StopOwnedBitcoindGracefullyAsync(
            currentBitcoind, workDir, datadir, rpcUrl, rpc);
        currentBitcoind = null;
        updateOwnedBitcoind(null);

        await WaitForHttpStatusAsync(
            $"http://127.0.0.1:{apiPort}/healthz", expectedStatusCode: 200,
            TimeSpan.FromSeconds(10));
        await WaitForHttpStatusAsync(
            $"http://127.0.0.1:{apiPort}/readyz", expectedStatusCode: 503,
            TimeSpan.FromSeconds(30));
        await WaitForLogAsync(
            logs,
            line => line.Contains("longpoll error", StringComparison.OrdinalIgnoreCase),
            "longpoll failure after Core stop",
            TimeSpan.FromSeconds(30));

        logs.Clear();
        currentBitcoind = StartOwnedBitcoind(datadir, rpcUrl, rpcUser, rpcPass);
        updateOwnedBitcoind(currentBitcoind);
        WaitForRpc(rpc, TimeSpan.FromSeconds(60));
        await VerifyHarnessChainIdentityAsync(rpc);
        await ChainGuard.VerifyAsync(
            new AppConfig { NetworkName = "regtest" }, rpc, CancellationToken.None);

        var reconnectJobTask = client.WaitForJobAsync(
            candidate => candidate.CleanJobs && candidate.JobId != beforeRestart.JobId,
            TimeSpan.FromSeconds(30));
        var generated = await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 1, payout });
        var generatedHash = generated[0].GetString()
            ?? throw new InvalidOperationException("Core restart generated no block hash");
        var reconnectedJob = await reconnectJobTask;
        if (!reconnectedJob.PrevHash.Equals(
                ToStratumNotifyPrevhash(generatedHash), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"post-restart job prevhash={reconnectedJob.PrevHash}, expected {generatedHash}");
        }

        await WaitForHttpStatusAsync(
            $"http://127.0.0.1:{apiPort}/readyz", expectedStatusCode: 200,
            TimeSpan.FromSeconds(30));
        await WaitForLogAsync(
            logs,
            line => line.Contains("ZMQ block notification received", StringComparison.OrdinalIgnoreCase),
            "ZMQ notification after Core restart",
            TimeSpan.FromSeconds(20));
        await WaitForLogAsync(
            logs,
            line => line.Contains("p2p fast peer ready", StringComparison.OrdinalIgnoreCase),
            "P2P reconnect after Core restart",
            TimeSpan.FromSeconds(20));

        var mined = MineStratumJob(
            reconnectedJob, extranonce1, maxNonces: 50_000_000,
            extranonce2: ExtranonceCounterHex(4, 1))
            ?? throw new InvalidOperationException("failed to mine pending-recovery block");
        var submittedVersionBits = SubmittedVersionBits(reconnectedJob, negotiatedMask);

        await StopOwnedBitcoindGracefullyAsync(
            currentBitcoind, workDir, datadir, rpcUrl, rpc);
        currentBitcoind = null;
        updateOwnedBitcoind(null);

        var accepted = await client.SubmitAsync(
            "restart-worker", reconnectedJob.JobId,
            mined.Extranonce2, mined.Ntime, mined.Nonce,
            submittedVersionBits);
        if (!accepted)
            throw new InvalidOperationException("gateway rejected the locally valid block while Core was offline");

        var pendingPath = await WaitForPendingBlockAsync(
            pendingDir, mined.HashHex, TimeSpan.FromSeconds(100));
        AssertPendingBlockFile(pendingPath, mined.HashHex);
        if (Directory.Exists(failedDir) && Directory.GetFiles(failedDir, "*.json").Length != 0)
            throw new InvalidOperationException("offline candidate was incorrectly archived as failed");

        TryKill(gateway);
        gateway = null;

        currentBitcoind = StartOwnedBitcoind(datadir, rpcUrl, rpcUser, rpcPass);
        updateOwnedBitcoind(currentBitcoind);
        WaitForRpc(rpc, TimeSpan.FromSeconds(60));
        await VerifyHarnessChainIdentityAsync(rpc);
        await ChainGuard.VerifyAsync(
            new AppConfig { NetworkName = "regtest" }, rpc, CancellationToken.None);

        logs.Clear();
        gateway = StartGateway(configPath, logs);
        await WaitHttpAsync(
            $"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));
        await WaitForChainTipAsync(
            rpc, mined.HashHex, TimeSpan.FromSeconds(45));
        await WaitForFileDeletionAsync(pendingPath, TimeSpan.FromSeconds(20));

        using var recoveredClient = new StratumMinerClient();
        await recoveredClient.ConnectAsync(stratumHost, stratumPort);
        _ = await recoveredClient.SubscribeAsync("regtest-recovered/1.0");
        await recoveredClient.AuthorizeAsync("recovered-worker", "x");
        var recoveredJob = await recoveredClient.WaitForJobAsync(TimeSpan.FromSeconds(30));
        if (!recoveredJob.PrevHash.Equals(
                ToStratumNotifyPrevhash(mined.HashHex), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "recovered gateway did not publish work on the recovered block tip");
        }

        Console.WriteLine(
            $"core restart recovered hash={mined.HashHex} " +
            "health=live ready=503->200 longpoll=retry ZMQ=reconnected P2P=reconnected pending=deleted");
    }
    finally
    {
        TryKill(gateway);
    }
}

static async Task WaitForHttpStatusAsync(
    string url,
    int expectedStatusCode,
    TimeSpan timeout)
{
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    var deadline = DateTime.UtcNow + timeout;
    int? lastStatus = null;
    Exception? lastError = null;
    while (DateTime.UtcNow < deadline)
    {
        try
        {
            using var response = await http.GetAsync(url);
            lastStatus = (int)response.StatusCode;
            if (lastStatus == expectedStatusCode)
                return;
        }
        catch (Exception ex)
        {
            lastError = ex;
        }
        await Task.Delay(150);
    }
    throw new TimeoutException(
        $"HTTP {url} did not reach {expectedStatusCode}; " +
        $"last_status={lastStatus?.ToString() ?? "none"} error={lastError?.Message ?? "none"}");
}

static async Task WaitForLogAsync(
    ConcurrentQueue<string> logs,
    Func<string, bool> predicate,
    string evidence,
    TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (logs.Any(predicate))
            return;
        await Task.Delay(100);
    }
    throw new TimeoutException($"gateway log did not contain {evidence}");
}

static async Task<string> WaitForPendingBlockAsync(
    string pendingDir,
    string expectedHash,
    TimeSpan timeout)
{
    var expectedPath = Path.Combine(pendingDir, expectedHash.ToLowerInvariant() + ".json");
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (File.Exists(expectedPath))
            return expectedPath;
        await Task.Delay(200);
    }
    throw new TimeoutException($"pending block was not persisted: {expectedPath}");
}

static void AssertPendingBlockFile(string path, string expectedHash)
{
    using var document = JsonDocument.Parse(File.ReadAllText(path));
    var root = document.RootElement;
    var hash = root.GetProperty("hash").GetString();
    var blockHex = root.GetProperty("blockHex").GetString();
    if (!string.Equals(hash, expectedHash, StringComparison.OrdinalIgnoreCase) ||
        string.IsNullOrEmpty(blockHex) || blockHex.Length < 160 ||
        !BitcoinEncoding.IsExactHex(blockHex))
        throw new InvalidOperationException("pending block file metadata or hex is invalid");

    var header = Hex.Decode(blockHex[..160]);
    var headerHash = Hash256.FromLittleEndian(BitcoinEncoding.DoubleSha256(header)).ToHex();
    if (!headerHash.Equals(expectedHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("pending block file header hash does not match its filename");
}

static async Task WaitForChainTipAsync(
    BitcoinRpcClient rpc,
    string expectedHash,
    TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    string? actual = null;
    while (DateTime.UtcNow < deadline)
    {
        var info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
        actual = info.GetProperty("bestblockhash").GetString();
        if (string.Equals(actual, expectedHash, StringComparison.OrdinalIgnoreCase))
            return;
        await Task.Delay(150);
    }
    throw new TimeoutException($"chain tip={actual}, expected recovered hash={expectedHash}");
}

static async Task WaitForFileDeletionAsync(string path, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (!File.Exists(path))
            return;
        await Task.Delay(100);
    }
    throw new TimeoutException($"recovered pending file was not deleted: {path}");
}

static void RunExtranonceConfigRangeChecks()
{
    for (var en1Size = 1; en1Size <= 4; en1Size++)
    {
        for (var en2Size = 1; en2Size <= 8; en2Size++)
        {
            var cfg = OfflineConfig(Path.Combine(
                Path.GetTempPath(), $"miningcore-btc-solo-en-{en1Size}-{en2Size}"));
            cfg.Stratum.Extranonce1Size = en1Size;
            cfg.Stratum.Extranonce2Size = en2Size;
            cfg.Stratum.MaxConnections = 32;
            cfg.Validate();
        }
    }

    foreach (var badEn1 in new[] { 0, 5 })
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), $"miningcore-btc-solo-bad-en1-{badEn1}"));
        cfg.Stratum.Extranonce1Size = badEn1;
        ExpectException<InvalidOperationException>(
            cfg.Validate,
            "stratum.extranonce1_size must be 1..4",
            $"extranonce1_size={badEn1}");
    }

    foreach (var badEn2 in new[] { 0, 9 })
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), $"miningcore-btc-solo-bad-en2-{badEn2}"));
        cfg.Stratum.Extranonce2Size = badEn2;
        ExpectException<InvalidOperationException>(
            cfg.Validate,
            "stratum.extranonce2_size must be 1..8",
            $"extranonce2_size={badEn2}");
    }

    Console.WriteLine("PASS extranonce config range checks (valid 4x8 + rejected 0/5 and 0/9)");
}

static byte[] MakeExtranonceBytes(int size, byte seed)
{
    var bytes = new byte[size];
    for (var i = 0; i < size; i++)
        bytes[i] = (byte)(seed + i);
    return bytes;
}

static async Task RunDirectExtranonceComboAsync(
    BitcoinRpcClient rpc,
    string payout,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    int en1Size,
    int en2Size)
{
    var label = $"direct en1={en1Size} en2={en2Size}";
    var cfg = BuildConfig(payout, rpcUrl, rpcUser, rpcPass, en1Size, en2Size);
    var builder = new JobBuilder(cfg);

    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();

    var gbt = await WaitForGbtAsync(rpc, requireTxCount: 0, TimeSpan.FromSeconds(15));
    var job = builder.FromGbt(gbt, TemplateSource.Startup);
    if (!job.Ready)
        throw new InvalidOperationException($"{label}: job not ready");

    var reserved = en1Size + en2Size;
    var assembledScaffold = job.Coinbase1.Length + reserved + job.Coinbase2.Length;
    if (assembledScaffold <= 0)
        throw new InvalidOperationException($"{label}: empty coinbase scaffold");

    var en1 = MakeExtranonceBytes(en1Size, 0xA0);
    var en2Bytes = MakeExtranonceBytes(en2Size, 0x50);
    var en2Hex = Hex.Encode(en2Bytes);
    var prefix = new byte[job.Coinbase1.Length + en1.Length];
    Buffer.BlockCopy(job.Coinbase1, 0, prefix, 0, job.Coinbase1.Length);
    Buffer.BlockCopy(en1, 0, prefix, job.Coinbase1.Length, en1.Length);

    var emptyShare = ShareValidator.Validate(job, prefix, new ShareSubmit
    {
        Extranonce2 = "",
        Ntime = Hex.U32BeHex(job.Ntime),
        Nonce = Hex.U32BeHex(1)
    }, Enumerable.Repeat((byte)0xff, 32).ToArray());
    if (emptyShare.Accepted || emptyShare.IsBlock)
        throw new InvalidOperationException($"{label}: empty extranonce2 was accepted");

    var shareTarget = Enumerable.Repeat((byte)0xff, 32).ToArray();
    var mined = MineBlock(job, prefix, en2Hex, shareTarget, maxNonces: 50_000_000);
    if (mined == null)
        throw new InvalidOperationException($"{label}: failed to mine within nonce budget");
    var minedValue = mined.Value;
    if (!minedValue.Accepted)
        throw new InvalidOperationException($"{label}: share was not accepted");
    if (!minedValue.IsBlock || minedValue.BlockCandidate == null)
        throw new InvalidOperationException($"{label}: mined share is not a full block");

    var minedHashHex = minedValue.Hash.ToHex();
    var submitResult = await rpc.SubmitBlockAsync(minedValue.BlockCandidate);
    if (submitResult != null && submitResult != "duplicate")
        throw new InvalidOperationException($"{label}: submitblock rejected: {submitResult}");

    await Task.Delay(150);
    var tipAfter = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightAfter = tipAfter.GetProperty("blocks").GetInt32();
    var bestAfter = tipAfter.GetProperty("bestblockhash").GetString()!;
    if (heightAfter != heightBefore + 1)
        throw new InvalidOperationException($"{label}: height {heightBefore} → {heightAfter}");
    if (!bestAfter.Equals(minedHashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"{label}: tip={bestAfter} mined={minedHashHex}");

    await AssertOnChainExtranonceAsync(rpc, minedHashHex, en1, en2Bytes, label);
    Console.WriteLine(
        $"PASS {label} height={heightAfter} hash={minedHashHex} share=accepted block=accepted");

    if ((en1Size == 1 && en2Size == 1) || (en1Size == 4 && en2Size == 8))
        await AssertMismatchedExtranonceRejectedAsync(rpc, payout, rpcUrl, rpcUser, rpcPass, en1Size, en2Size);
}

static async Task AssertMismatchedExtranonceRejectedAsync(
    BitcoinRpcClient rpc,
    string payout,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    int en1Size,
    int en2Size)
{
    var label = $"mismatch en1={en1Size} reserved-en2={en2Size}";
    var cfg = BuildConfig(payout, rpcUrl, rpcUser, rpcPass, en1Size, en2Size);
    var builder = new JobBuilder(cfg);
    var gbt = await WaitForGbtAsync(rpc, requireTxCount: 0, TimeSpan.FromSeconds(15));
    var job = builder.FromGbt(gbt, TemplateSource.Startup);

    var en1 = MakeExtranonceBytes(en1Size, 0xA0);
    var wrongEn2 = MakeExtranonceBytes(en2Size + 1, 0x50);
    var prefix = new byte[job.Coinbase1.Length + en1.Length];
    Buffer.BlockCopy(job.Coinbase1, 0, prefix, 0, job.Coinbase1.Length);
    Buffer.BlockCopy(en1, 0, prefix, job.Coinbase1.Length, en1.Length);

    var mined = MineBlock(job, prefix, Hex.Encode(wrongEn2), Enumerable.Repeat((byte)0xff, 32).ToArray(), 50_000_000);
    if (mined is not { IsBlock: true, BlockCandidate: not null })
        throw new InvalidOperationException($"{label}: failed to assemble a malformed candidate");

    string? submitResult;
    try
    {
        submitResult = await rpc.SubmitBlockAsync(mined.Value.BlockCandidate);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"PASS {label} submitblock rejected ({ex.GetType().Name}: {ex.Message})");
        return;
    }

    if (submitResult == null || submitResult == "duplicate")
        throw new InvalidOperationException($"{label}: Core accepted a coinbase with the wrong extranonce2 length");

    Console.WriteLine($"PASS {label} submitblock rejected ({submitResult})");
}

static async Task AssertOnChainExtranonceAsync(
    BitcoinRpcClient rpc,
    string blockHash,
    byte[] en1,
    byte[] en2,
    string label)
{
    var block = await rpc.CallAsync<JsonElement>("getblock", new object[] { blockHash, 2 });
    var coinbaseScript = block.GetProperty("tx")[0].GetProperty("vin")[0].GetProperty("coinbase").GetString()
        ?? throw new InvalidOperationException($"{label}: missing coinbase scriptSig");
    var expectedTail = Hex.Encode(en1) + Hex.Encode(en2);
    if (!coinbaseScript.EndsWith(expectedTail, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException(
            $"{label}: coinbase scriptSig does not end with en1||en2 ({expectedTail}); script={coinbaseScript}");
}

static async Task RunStratumExtranonceComboAsync(
    BitcoinRpcClient rpc,
    string payout,
    string workDir,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    string stratumHost,
    int en1Size,
    int en2Size)
{
    var label = $"stratum en1={en1Size} en2={en2Size}";
    var stratumPort = GetFreeTcpPort();
    var apiPort = GetFreeTcpPort();
    var comboDir = Path.Combine(workDir, $"gw-en1{en1Size}-en2{en2Size}");
    Directory.CreateDirectory(comboDir);

    var configPath = await WriteRuntimeConfigAsync(
        comboDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort,
        extranonce1Size: en1Size, extranonce2Size: en2Size);
    var gateway = StartGateway(configPath);
    try
    {
        await WaitHttpAsync($"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));

    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();

    using var client = new StratumMinerClient();
    await client.ConnectAsync(stratumHost, stratumPort);
    var (en1Hex, advertisedEn2) = await client.SubscribeAsync($"regtest-en-{en1Size}-{en2Size}/1.0");
    if (en1Hex.Length != en1Size * 2)
        throw new InvalidOperationException($"{label}: subscribe en1 hex length={en1Hex.Length}, expected {en1Size * 2}");
    if (advertisedEn2 != en2Size)
        throw new InvalidOperationException($"{label}: subscribe en2_size={advertisedEn2}, expected {en2Size}");

    await client.AuthorizeAsync("worker1", "x");
    await client.WaitForDifficultyAsync(TimeSpan.FromSeconds(5));
    var job = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));

    var invalidExtranonces = new (string Case, string Value)[]
    {
        ("too long", new string('0', (en2Size + 1) * 2)),
        ("non-hex", new string('g', en2Size * 2))
    };
    foreach (var invalid in invalidExtranonces)
    {
        var error = await client.SubmitExpectErrorAsync(
            "worker1", job.JobId, invalid.Value, job.NTime, "00000001");
        if (!error.Contains("extranonce2", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"{label}: {invalid.Case} en2 was not rejected as extranonce2: {error}");
        }
    }

    var en2Hex = Hex.Encode(MakeExtranonceBytes(en2Size, 0x01));
    var mined = MineStratumJob(job, en1Hex, maxNonces: 50_000_000, extranonce2: en2Hex);
    if (mined == null)
        throw new InvalidOperationException($"{label}: stratum mine failed");

    var accepted = await client.SubmitAsync(
        "worker1", job.JobId, mined.Value.Extranonce2, mined.Value.Ntime, mined.Value.Nonce);
    if (!accepted)
        throw new InvalidOperationException($"{label}: mining.submit not accepted");

    var deadline = DateTime.UtcNow.AddSeconds(20);
    string? best = null;
    var heightAfter = heightBefore;
    while (DateTime.UtcNow < deadline)
    {
        var tip = await rpc.CallAsync<JsonElement>("getblockchaininfo");
        heightAfter = tip.GetProperty("blocks").GetInt32();
        best = tip.GetProperty("bestblockhash").GetString();
        if (heightAfter >= heightBefore + 1 &&
            best != null &&
            best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
            break;
        await Task.Delay(150);
    }

    if (heightAfter < heightBefore + 1)
        throw new InvalidOperationException($"{label}: chain did not advance ({heightBefore}→{heightAfter})");
    if (best == null || !best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"{label}: tip={best} mined={mined.Value.HashHex}");

        await AssertOnChainExtranonceAsync(rpc, best, Hex.Decode(en1Hex), Hex.Decode(en2Hex), label);
        Console.WriteLine(
            $"PASS {label} subscribe en1={en1Hex} en2_size={advertisedEn2} height={heightAfter} hash={best}");
    }
    finally
    {
        TryKill(gateway);
    }
}

static async Task RunDirectPathAsync(
    BitcoinRpcClient rpc,
    string payout,
    int requireTxCount,
    string label,
    IReadOnlyList<string>? expectedTxids = null,
    bool requireAllExpectedTxids = false,
    TimeSpan? gbtTimeout = null)
{
    var cfg = BuildConfig(payout, "http://127.0.0.1:18443", "regtest", "regtestpass");
    var builder = new JobBuilder(cfg);

    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();
    var bestBefore = tipBefore.GetProperty("bestblockhash").GetString()!;

    var gbt = await WaitForGbtAsync(
        rpc, requireTxCount, gbtTimeout ?? TimeSpan.FromSeconds(15));

    if (string.IsNullOrEmpty(gbt.DefaultWitnessCommitment) && gbt.TransactionCount > 0)
        throw new InvalidOperationException("GBT missing default_witness_commitment with txs present");
    if (requireTxCount > 0 && string.IsNullOrEmpty(gbt.DefaultWitnessCommitment))
        throw new InvalidOperationException("multi-tx GBT must include default_witness_commitment (segwit)");

    // Confirm at least one GBT entry is a true witness tx when we seeded bech32 spends
    if (requireTxCount > 0)
    {
        var hasWitnessTransactions = GbtHasWitnessTransactions(gbt);
        Console.WriteLine(
            $"GBT witness txs present={hasWitnessTransactions} total={gbt.TransactionCount}");
        if (!hasWitnessTransactions)
            throw new InvalidOperationException("expected at least one segwit mempool tx (txid != wtxid) in GBT");
    }

    var job = builder.FromGbt(gbt, TemplateSource.Startup);
    if (!job.Ready)
        throw new InvalidOperationException("job not ready");
    if (job.TransactionCount < requireTxCount)
        throw new InvalidOperationException($"job txs={job.TransactionCount} < required {requireTxCount}");

    Console.WriteLine(
        $"[{label}] job height={job.Height} nbits={job.NbitsHex} txs={job.TransactionCount} " +
        $"merkleBranches={job.MerkleBranchesLe.Count} witness={job.HasWitnessCommitment}");

    // Fixed extranonce for deterministic assembly
    var en1 = Hex.Decode("01020304");
    var en2 = "05060708";
    var prefix = new byte[job.Coinbase1.Length + en1.Length];
    Buffer.BlockCopy(job.Coinbase1, 0, prefix, 0, job.Coinbase1.Length);
    Buffer.BlockCopy(en1, 0, prefix, job.Coinbase1.Length, en1.Length);

    // Share target: accept anything (diff ~0) so we only care about block target for isBlock
    var shareTarget = Enumerable.Repeat((byte)0xff, 32).ToArray();

    var mined = MineBlock(job, prefix, en2, shareTarget, maxNonces: 50_000_000);
    if (mined == null)
        throw new InvalidOperationException("failed to mine block within nonce budget (regtest target unexpectedly hard?)");
    var minedValue = mined.Value;
    var minedHashHex = minedValue.Hash.ToHex();

    Console.WriteLine($"[{label}] mined hash={minedHashHex} actualDiff={minedValue.ActualDiff:F6} isBlock={minedValue.IsBlock}");
    if (!minedValue.IsBlock || minedValue.BlockCandidate == null)
        throw new InvalidOperationException("mined share is not a full block");

    AssertTransactionsEmbedded(minedValue.BlockCandidate, job.Transactions);

    var submitResult = await rpc.SubmitBlockAsync(minedValue.BlockCandidate);
    if (submitResult != null && submitResult != "duplicate")
        throw new InvalidOperationException($"submitblock rejected: {submitResult}");

    Console.WriteLine($"[{label}] submitblock ok result={submitResult ?? "null(accepted)"}");

    // Confirm on active chain
    await Task.Delay(200);
    var tipAfter = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightAfter = tipAfter.GetProperty("blocks").GetInt32();
    var bestAfter = tipAfter.GetProperty("bestblockhash").GetString()!;

    if (heightAfter != heightBefore + 1)
        throw new InvalidOperationException($"height did not advance: {heightBefore} → {heightAfter}");
    if (!bestAfter.Equals(minedHashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"bestblockhash mismatch: chain={bestAfter} mined={minedHashHex}");

    var block = await rpc.CallAsync<JsonElement>("getblock", new object[] { minedHashHex, 1 });
    var conf = block.GetProperty("confirmations").GetInt32();
    var blockWeight = block.GetProperty("weight").GetInt32();
    if (conf < 1)
        throw new InvalidOperationException($"block confirmations={conf}");
    if (blockWeight > 4_000_000)
        throw new InvalidOperationException($"block weight exceeds consensus limit: {blockWeight}");

    var blockTxids = block.GetProperty("tx").EnumerateArray().Select(x => x.GetString()!).ToList();
    if (blockTxids.Count != job.TransactionCount + 1)
        throw new InvalidOperationException(
            $"block tx count mismatch: chain={blockTxids.Count} expected={job.TransactionCount + 1}");

    AssertTxidsInBlock(
        blockTxids, expectedTxids, gbt, requireAllExpectedTxids);

    var coinbaseTxid = blockTxids[0];
    Console.WriteLine(
        $"[{label}] on-chain height={heightAfter} conf={conf} txs={blockTxids.Count} weight={blockWeight} " +
        $"coinbase={coinbaseTxid} prev={bestBefore}");
}

static async Task RunStratumPathAsync(
    BitcoinRpcClient rpc,
    string host,
    int port,
    int apiPort,
    IReadOnlyList<string>? expectedTxids = null,
    bool requireAllExpectedTxids = false,
    int expectedExtranonce2Size = 4,
    int? expectedTransactionCount = null,
    TimeSpan? gbtTimeout = null)
{
    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();

    // Confirm GBT still has mempool before mining (gateway should have built the same tip)
    var requiredTransactions = expectedTransactionCount ??
        (expectedTxids is { Count: > 0 } ? 1 : 0);
    var gbt = await WaitForGbtAsync(
        rpc, requireTxCount: requiredTransactions,
        gbtTimeout ?? TimeSpan.FromSeconds(15));
    Console.WriteLine($"stratum pre-mine GBT txs={gbt.TransactionCount} height={gbt.Height}");
    await WaitForPublishedGatewayJobAsync(
        apiPort,
        requireTxCount: requiredTransactions,
        expectedHeight: gbt.Height,
        gbtTimeout ?? TimeSpan.FromSeconds(45));

    using var client = new StratumMinerClient();
    await client.ConnectAsync(host, port);
    var negotiatedMask = await client.ConfigureVersionRollingAsync("1fffe000");
    Console.WriteLine($"stratum version-rolling mask={negotiatedMask}");
    var (en1, advertisedExtranonce2Size) = await client.SubscribeAsync("regtest-miner/1.0");
    if (advertisedExtranonce2Size != expectedExtranonce2Size)
    {
        throw new InvalidOperationException(
            $"stratum advertised extranonce2_size={advertisedExtranonce2Size}, " +
            $"expected {expectedExtranonce2Size}");
    }
    await client.AuthorizeAsync("worker1", "x");
    await client.WaitForDifficultyAsync(TimeSpan.FromSeconds(5));
    var publicJob = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));

    if (expectedTxids is { Count: > 0 } && publicJob.MerkleBranch.Count == 0)
        throw new InvalidOperationException(
            "stratum job has empty merkle branch but mempool txs were expected (gateway template missing txs)");

    Console.WriteLine(
        $"stratum job id={publicJob.JobId} prev={publicJob.PrevHash} nbits={publicJob.NBits} en1={en1} " +
        $"merkleBranches={publicJob.MerkleBranch.Count}");

    var job = publicJob;

    // Build coinbase + header using same rules as ShareValidator (via notify fields)
    var extranonce2 = Hex.Encode(MakeExtranonceBytes(expectedExtranonce2Size, 0x01));
    var mined = MineStratumJob(
        job, en1, maxNonces: 50_000_000, extranonce2: extranonce2);
    if (mined == null)
        throw new InvalidOperationException("stratum mine failed within nonce budget");

    Console.WriteLine($"stratum mined ntime={mined.Value.Ntime} nonce={mined.Value.Nonce} hash={mined.Value.HashHex}");

    var submittedVersionBits = SubmittedVersionBits(job, negotiatedMask);
    var accepted = await client.SubmitAsync(
        "worker1",
        job.JobId,
        mined.Value.Extranonce2,
        mined.Value.Ntime,
        mined.Value.Nonce,
        submittedVersionBits);

    if (!accepted)
        throw new InvalidOperationException("mining.submit not accepted by gateway");

    Console.WriteLine("mining.submit accepted");

    // Wait for gateway submitblock + chain advance
    var deadline = DateTime.UtcNow.AddSeconds(30);
    string? best = null;
    int heightAfter = heightBefore;
    while (DateTime.UtcNow < deadline)
    {
        var tip = await rpc.CallAsync<JsonElement>("getblockchaininfo");
        heightAfter = tip.GetProperty("blocks").GetInt32();
        best = tip.GetProperty("bestblockhash").GetString();
        if (heightAfter >= heightBefore + 1 &&
            best != null &&
            best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
            break;
        await Task.Delay(250);
    }

    if (heightAfter < heightBefore + 1)
        throw new InvalidOperationException($"chain height did not advance after stratum submit ({heightBefore}→{heightAfter})");
    if (best == null || !best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"tip hash mismatch after stratum: tip={best} mined={mined.Value.HashHex}");

    var block = await rpc.CallAsync<JsonElement>("getblock", new object[] { best, 1 });
    var blockTxids = block.GetProperty("tx").EnumerateArray().Select(x => x.GetString()!).ToList();
    if (expectedTxids is { Count: > 0 } && blockTxids.Count < 2)
        throw new InvalidOperationException($"stratum block has no mempool txs (txcount={blockTxids.Count})");
    AssertTxidsInBlock(
        blockTxids, expectedTxids, gbt,
        requireAllExpectedTxids: requireAllExpectedTxids);

    // Dashboard API must record the block
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    var blocksJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/blocks");
    if (!blocksJson.Contains(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase) &&
        !blocksJson.Contains("submitted", StringComparison.OrdinalIgnoreCase) &&
        !blocksJson.Contains("duplicate", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine($"warn: /api/blocks may not yet list hash (payload={blocksJson})");
    }
    else
    {
        Console.WriteLine("dashboard /api/blocks observed submit");
    }

    var workersJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/workers");
    using (var workersDoc = JsonDocument.Parse(workersJson))
    {
        var workerSeen = workersDoc.RootElement.EnumerateArray().Any(worker =>
            worker.TryGetProperty("extranonce1", out var en1El) &&
            string.Equals(en1El.GetString(), en1, StringComparison.OrdinalIgnoreCase) &&
            worker.TryGetProperty("assigned_difficulty", out var diffEl) &&
            diffEl.GetDouble() > 0);
        if (!workerSeen)
            throw new InvalidOperationException($"dashboard worker metrics missing en1={en1}: {workersJson}");
    }

    var sharesJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/shares");
    using (var sharesDoc = JsonDocument.Parse(sharesJson))
    {
        var shareSeen = sharesDoc.RootElement.EnumerateArray().Any(share =>
            share.TryGetProperty("extranonce1", out var en1El) &&
            string.Equals(en1El.GetString(), en1, StringComparison.OrdinalIgnoreCase) &&
            share.TryGetProperty("actual_diff", out var diffEl) &&
            diffEl.GetDouble() > 0 &&
            share.TryGetProperty("hash", out var hashEl) &&
            string.Equals(
                hashEl.GetString(), mined.Value.HashHex, StringComparison.OrdinalIgnoreCase));
        if (!shareSeen)
            throw new InvalidOperationException($"dashboard share metrics missing en1={en1}: {sharesJson}");
    }
    Console.WriteLine("dashboard worker/share metrics observed accepted share");

    Console.WriteLine($"stratum on-chain height={heightAfter} hash={best} txs={blockTxids.Count}");

}

static async Task RunStratumLifecyclePathAsync(
    BitcoinRpcClient rpc,
    string host,
    int port,
    int apiPort,
    string payout,
    int minerCount,
    int extranonce2Size = 4,
    bool requireInitialMempool = false,
    IReadOnlyList<string>? expectedInitialTxids = null)
{
    var requiredTransactions = expectedInitialTxids is { Count: > 0 }
        ? expectedInitialTxids.Count
        : requireInitialMempool ? 1 : 0;
    var initialGbt = await WaitForGbtAsync(
        rpc, requireTxCount: requiredTransactions, TimeSpan.FromSeconds(20));
    await WaitForPublishedGatewayJobAsync(
        apiPort,
        requireTxCount: requiredTransactions,
        expectedHeight: initialGbt.Height,
        TimeSpan.FromSeconds(45));

    var clients = Enumerable.Range(0, minerCount).Select(_ => new StratumMinerClient()).ToArray();
    try
    {
        var setup = clients.Select(async (client, index) =>
        {
            await client.ConnectAsync(host, port);
            var negotiatedMask = await client.ConfigureVersionRollingAsync("1fffe000");
            var (en1, advertisedExtranonce2Size) =
                await client.SubscribeAsync($"regtest-lifecycle/{index}");
            if (advertisedExtranonce2Size != extranonce2Size)
            {
                throw new InvalidOperationException(
                    $"lifecycle advertised extranonce2_size={advertisedExtranonce2Size}, " +
                    $"expected {extranonce2Size}");
            }
            await client.AuthorizeAsync($"worker{index}", "x");
            var job = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));
            return (
                Index: index,
                Extranonce1: en1,
                Job: job,
                VersionBits: SubmittedVersionBits(job, negotiatedMask));
        }).ToArray();
        var initial = await Task.WhenAll(setup);
        var firstJobId = initial[0].Job.JobId;
        if (initial.Any(x => x.Job.JobId != firstJobId || !x.Job.CleanJobs))
            throw new InvalidOperationException("miners did not start on the same clean job");
        if (requireInitialMempool && initial.Any(x => x.Job.MerkleBranch.Count == 0))
            throw new InvalidOperationException("lifecycle boundary job did not contain witness mempool transactions");

        // Lifecycle mode sets a target above uint256, so any header is a share. Pick
        // headers that deliberately miss the regtest network target to avoid creating
        // a competing block when exercising old-job acceptance.
        var lateExtranonce2 = ExtranonceCounterHex(extranonce2Size, 1);
        var staleExtranonce2 = ExtranonceCounterHex(extranonce2Size, 2);
        var lateShares = initial.Select(x =>
            MineNonBlockStratumShare(x.Job, x.Extranonce1, lateExtranonce2)).ToArray();
        var staleShare = MineNonBlockStratumShare(
            initial[0].Job, initial[0].Extranonce1, staleExtranonce2);

        // Arm notification reads before generating. The RPC can return after the
        // gateway has already completed the clean broadcast, so awaiting it first
        // would consume the late-share grace window on a busy test host.
        var firstCleanJobs = clients.Select((client, index) =>
            client.WaitForJobAsync(
                candidate => candidate.CleanJobs && candidate.JobId != firstJobId,
                TimeSpan.FromSeconds(10))).ToArray();
        var burstStarted = Stopwatch.GetTimestamp();
        var generatedTask = rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 3, payout });

        // Once publication has retired the old job, submit work computed immediately
        // before the burst. This must be accepted during late_share_grace_ms.
        await Task.WhenAny(firstCleanJobs);
        var lateSubmitStarted = Stopwatch.GetTimestamp();
        var lateSubmits = clients.Select(async (client, index) =>
        {
            var accepted = await client.SubmitAsync(
                $"worker{index}", firstJobId, lateShares[index].Extranonce2,
                lateShares[index].Ntime, lateShares[index].Nonce, initial[index].VersionBits);
            return (Accepted: accepted, CompletedTimestamp: Stopwatch.GetTimestamp());
        }).ToArray();
        var lateResults = await Task.WhenAll(lateSubmits);
        if (lateResults.Any(x => !x.Accepted))
            throw new InvalidOperationException("one or more old-job shares were rejected during clean grace");
        var firstReceived = await Task.WhenAll(firstCleanJobs);

        var generated = await generatedTask;
        var hashes = generated.EnumerateArray().Select(x => x.GetString()!).ToArray();
        if (hashes.Length != 3)
            throw new InvalidOperationException($"generatetoaddress returned {hashes.Length} hashes, expected 3");
        if (expectedInitialTxids is { Count: > 0 })
        {
            var firstBlock = await rpc.CallAsync<JsonElement>(
                "getblock", new object[] { hashes[0], 1 });
            var firstBlockTxids = firstBlock.GetProperty("tx")
                .EnumerateArray()
                .Select(transaction => transaction.GetString()!)
                .ToArray();
            AssertTxidsInBlock(
                firstBlockTxids,
                expectedInitialTxids,
                initialGbt,
                requireAllExpectedTxids: true);
            Console.WriteLine(
                $"lifecycle witness block hash={hashes[0]} txs={firstBlockTxids.Length}");
        }
        var finalNotifyPrevhash = ToStratumNotifyPrevhash(hashes[^1]);

        var finalJobs = firstReceived.Select(async (firstJob, index) =>
        {
            var job = firstJob.PrevHash.Equals(
                finalNotifyPrevhash, StringComparison.OrdinalIgnoreCase)
                ? firstJob
                : await clients[index].WaitForJobAsync(
                    candidate => candidate.CleanJobs &&
                        candidate.PrevHash.Equals(finalNotifyPrevhash, StringComparison.OrdinalIgnoreCase),
                    TimeSpan.FromSeconds(10));
            return (Index: index, Job: job);
        }).ToArray();
        var lateAckFirstMs = StopwatchTicksToMilliseconds(
            lateResults.Min(x => x.CompletedTimestamp) - lateSubmitStarted);
        var lateAckLastMs = StopwatchTicksToMilliseconds(
            lateResults.Max(x => x.CompletedTimestamp) - lateSubmitStarted);

        var received = await Task.WhenAll(finalJobs);
        var arrivalTicks = received.Select(x => x.Job.ReceivedTimestamp).ToArray();
        var firstArrivalMs = StopwatchTicksToMilliseconds(arrivalTicks.Min() - burstStarted);
        var lastArrivalMs = StopwatchTicksToMilliseconds(arrivalTicks.Max() - burstStarted);
        var spreadMs = StopwatchTicksToMilliseconds(arrivalTicks.Max() - arrivalTicks.Min());
        if (spreadMs >= 1500)
            throw new InvalidOperationException($"clean fan-out spread {spreadMs:F3}ms exceeded 1500ms barrier");
        if (clients.Any(x => x.NotificationCount < 2 || x.CleanNotificationCount < 2))
            throw new InvalidOperationException("one or more miners missed the post-burst clean notification");

        Console.WriteLine(
            $"burst blocks=3 miners={minerCount} final_job={received[0].Job.JobId} " +
            $"first_clean_ms={firstArrivalMs:F3} last_clean_ms={lastArrivalMs:F3} spread_ms={spreadMs:F3} " +
            $"late_ack_first_ms={lateAckFirstMs:F3} late_ack_last_ms={lateAckLastMs:F3}");

        // Socket receipt is later than server WriteAsync completion, so grace is
        // guaranteed to have elapsed 2.4s after the last miner observes clean.
        await Task.Delay(2400);
        var staleError = await clients[0].SubmitExpectErrorAsync(
            "worker0", firstJobId, staleShare.Extranonce2,
            staleShare.Ntime, staleShare.Nonce, initial[0].VersionBits);
        if (!staleError.Contains("Stale job", StringComparison.OrdinalIgnoreCase) ||
            staleError.Contains("Job not found", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"retired job classification changed: {staleError}");

        var unknownError = await clients[0].SubmitExpectErrorAsync(
            "worker0", "ffffffffffffffff", staleShare.Extranonce2,
            staleShare.Ntime, staleShare.Nonce, initial[0].VersionBits);
        if (!unknownError.Contains("Job not found", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"unknown job classification changed: {unknownError}");

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var metricsDeadline = DateTime.UtcNow.AddSeconds(3);
        JsonElement stats = default;
        while (DateTime.UtcNow < metricsDeadline)
        {
            using var statsDoc = JsonDocument.Parse(
                await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/stats"));
            stats = statsDoc.RootElement.Clone();
            if (stats.GetProperty("shares_late").GetInt64() >= minerCount &&
                stats.GetProperty("shares_stale").GetInt64() >= 1 &&
                stats.GetProperty("shares_unknown_job").GetInt64() >= 1 &&
                stats.GetProperty("share_accepted_ack_written").GetInt64() >= minerCount)
                break;
            await Task.Delay(50);
        }

        if (stats.ValueKind != JsonValueKind.Object ||
            stats.GetProperty("shares_late").GetInt64() < minerCount ||
            stats.GetProperty("shares_stale").GetInt64() < 1 ||
            stats.GetProperty("shares_unknown_job").GetInt64() < 1 ||
            stats.GetProperty("clean_broadcast_client_timeouts").GetInt64() != 0 ||
            stats.GetProperty("share_accepted_ack_written").GetInt64() < minerCount)
            throw new InvalidOperationException($"lifecycle metrics did not converge: {stats}");

        Console.WriteLine(
            $"lifecycle metrics late={stats.GetProperty("shares_late").GetInt64()} " +
            $"stale={stats.GetProperty("shares_stale").GetInt64()} " +
            $"unknown={stats.GetProperty("shares_unknown_job").GetInt64()} " +
            $"clean={stats.GetProperty("clean_broadcasts").GetInt64()} " +
            $"timeouts={stats.GetProperty("clean_broadcast_client_timeouts").GetInt64()} " +
            $"validate_avg_ms={stats.GetProperty("share_validation_avg_ms").GetDouble():F3} " +
            $"ack_queue_avg_ms={stats.GetProperty("share_accepted_ack_queue_avg_ms").GetDouble():F3} " +
            $"ack_write_avg_ms={stats.GetProperty("share_accepted_ack_write_avg_ms").GetDouble():F3}");
    }
    finally
    {
        foreach (var client in clients)
            client.Dispose();
    }
}

static string ExtranonceCounterHex(int size, byte value)
{
    if (size is < 1 or > 8)
        throw new ArgumentOutOfRangeException(nameof(size));
    var bytes = new byte[size];
    bytes[^1] = value;
    return Hex.Encode(bytes);
}

static string SubmittedVersionBits(StratumJob job, string negotiatedMask) =>
    (Convert.ToUInt32(job.Version, 16) & Convert.ToUInt32(negotiatedMask, 16))
    .ToString("x8", System.Globalization.CultureInfo.InvariantCulture);

static async Task RunRealP2pFastPolicyPathAsync(
    BitcoinRpcClient rpc,
    string regtestMiningAddress,
    string workDir)
{
    // ChainGuard correctly forbids starting a mainnet gateway against regtest. Test the
    // mainnet-only empty-fast policy in-process instead, using a genuine Core-mined
    // header and a genuine follow-up GBT from this isolated regtest node.
    var cfg = new AppConfig
    {
        NetworkName = "regtest",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = "http://127.0.0.1:18443",
            RpcUser = "regtest",
            RpcPassword = "regtestpass"
        },
        Coinbase = new CoinbaseConfig { Address = regtestMiningAddress },
        Runtime = new RuntimeConfig { DataDir = Path.Combine(workDir, "p2p-fast-policy-data") },
        Difficulty = new DifficultyConfig { Min = 1, Max = 1e12, Default = 1 }
    };
    cfg.Validate();
    // Keep regtest address/PoW semantics while selecting the mainnet-only fast-path
    // policy. Normal startup does not mutate the validated network name, and ChainGuard
    // rejects a mainnet/regtest RPC mismatch.
    cfg.NetworkName = "mainnet";
    var metrics = new MetricsStore();
    var submitQueue = new BlockSubmitQueue(cfg, rpc, metrics);
    var engine = new TemplateEngine(cfg, rpc, metrics, submitQueue);

    async Task<JobNotify> DispatchOneAsync()
    {
        JobNotify captured = default;
        var received = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await engine.DispatchNotificationsAsync(notify =>
            {
                captured = notify;
                received = true;
                cts.Cancel();
            }, cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        if (!received)
            throw new TimeoutException("timed out waiting for the P2P-fast job notification");
        return captured;
    }

    await engine.RefreshDirectAsync(TemplateSource.Startup, CancellationToken.None);
    var initial = await DispatchOneAsync();
    var generated = await rpc.CallAsync<JsonElement>(
        "generatetoaddress", new object[] { 1, regtestMiningAddress });
    var blockHash = generated[0].GetString()
        ?? throw new InvalidOperationException("real P2P-fast policy block hash missing");
    var header = await rpc.CallAsync<JsonElement>("getblockheader", new object[] { blockHash, true });
    var prevhash = header.GetProperty("previousblockhash").GetString()!;
    var height = header.GetProperty("height").GetUInt32();
    var blockTime = header.GetProperty("time").GetUInt32();
    var nbits = Convert.ToUInt32(header.GetProperty("bits").GetString(), 16);
    var blockVersion = header.GetProperty("version").GetUInt32();

    await engine.HandleP2pFastAnnouncementAsync(
        prevhash, blockHash, blockTime, height, nbits, blockVersion, CancellationToken.None);
    var fast = engine.ActiveMiningJob;
    if (fast.Source != TemplateSource.P2pFast || fast.Height != height + 1 ||
        !fast.PrevhashBe.Equals(blockHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real Core header did not publish mainnet P2P-fast work");

    // Do not dispatch the fast notify yet. The full Core GBT should coalesce over it,
    // retain clean=true, and become authoritative without exposing an intermediate job.
    await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
    var merged = await DispatchOneAsync();
    if (merged.Job.Source != TemplateSource.ZmqHashblock || !merged.CleanJobs ||
        merged.Job.Height != fast.Height ||
        !merged.Job.PrevhashBe.Equals(fast.PrevhashBe, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real GBT did not safely supersede P2P-fast clean work");
    if (engine.FindJob(initial.Job.JobId).Status != JobLookupStatus.RetiredWithinGrace ||
        engine.FindJob(fast.JobId).Status != JobLookupStatus.RetiredWithinGrace)
        throw new InvalidOperationException("real P2P-fast/full-GBT merge lost retired jobs");

    Console.WriteLine(
        $"real P2P-fast policy header={blockHash} fast_job={fast.JobId} " +
        $"full_job={merged.Job.JobId} merged_clean={merged.CleanJobs}");

    // Advance Core once more, then mine the child directly from the P2P-fast
    // coinbase-only job before any full GBT can replace it.
    var generatedParent = await rpc.CallAsync<JsonElement>(
        "generatetoaddress", new object[] { 1, regtestMiningAddress });
    var parentHash = generatedParent[0].GetString()
        ?? throw new InvalidOperationException("P2P-fast mining parent hash missing");
    var parentHeader = await rpc.CallAsync<JsonElement>(
        "getblockheader", new object[] { parentHash, true });
    var parentPrevhash = parentHeader.GetProperty("previousblockhash").GetString()!;
    var parentHeight = parentHeader.GetProperty("height").GetUInt32();
    var parentTime = parentHeader.GetProperty("time").GetUInt32();
    var parentNbits = Convert.ToUInt32(parentHeader.GetProperty("bits").GetString(), 16);
    var parentVersion = parentHeader.GetProperty("version").GetUInt32();

    await engine.HandleP2pFastAnnouncementAsync(
        parentPrevhash, parentHash, parentTime, parentHeight, parentNbits, parentVersion,
        CancellationToken.None);
    var miningJob = engine.ActiveMiningJob;
    if (miningJob.Source != TemplateSource.P2pFast ||
        miningJob.Height != parentHeight + 1 ||
        !miningJob.PrevhashBe.Equals(parentHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real parent header did not publish the mining P2P-fast job");
    if (miningJob.TransactionCount != 0 || miningJob.TransactionBytes != 0 ||
        miningJob.MerkleBranchesLe.Count != 0 || miningJob.MerkleBranchesHex.Count != 0 ||
        miningJob.HasWitnessCommitment || miningJob.WitnessCommitmentScriptHex != null)
        throw new InvalidOperationException("P2P-fast mining job was not coinbase-only");
    if (miningJob.CoinbaseValue != BitcoinEncoding.BlockSubsidySat(miningJob.Height))
        throw new InvalidOperationException("P2P-fast mining job coinbase subsidy changed");

    var fastNotify = await DispatchOneAsync();
    if (!fastNotify.CleanJobs || !ReferenceEquals(fastNotify.Job, miningJob))
        throw new InvalidOperationException("P2P-fast mining job was not dispatched as clean work");

    var extranonce1 = Hex.Decode("01020304");
    const string extranonce2 = "05060708";
    var coinbasePrefix = new byte[miningJob.Coinbase1.Length + extranonce1.Length];
    Buffer.BlockCopy(miningJob.Coinbase1, 0, coinbasePrefix, 0, miningJob.Coinbase1.Length);
    Buffer.BlockCopy(
        extranonce1, 0, coinbasePrefix, miningJob.Coinbase1.Length, extranonce1.Length);
    var shareTarget = Enumerable.Repeat((byte)0xff, 32).ToArray();
    var mined = MineBlock(
        miningJob, coinbasePrefix, extranonce2, shareTarget, maxNonces: 50_000_000)
        ?? throw new InvalidOperationException("failed to mine the P2P-fast coinbase-only job");
    if (!mined.IsBlock || mined.BlockHex == null)
        throw new InvalidOperationException("P2P-fast share did not assemble a full block");

    var minedHash = mined.Hash.ToHex();
    var submitResult = await rpc.SubmitBlockAsync(mined.BlockHex);
    if (submitResult != null)
        throw new InvalidOperationException($"P2P-fast submitblock rejected: {submitResult}");

    var chain = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    if (chain.GetProperty("blocks").GetUInt32() != miningJob.Height ||
        !chain.GetProperty("bestblockhash").GetString()!.Equals(
            minedHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("P2P-fast block did not become the active chain tip");

    var onChain = await rpc.CallAsync<JsonElement>(
        "getblock", new object[] { minedHash, 2 });
    if (onChain.GetProperty("height").GetUInt32() != miningJob.Height ||
        !onChain.GetProperty("previousblockhash").GetString()!.Equals(
            parentHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("P2P-fast on-chain block header changed");

    var transactions = onChain.GetProperty("tx");
    if (transactions.GetArrayLength() != 1)
        throw new InvalidOperationException(
            $"P2P-fast block contains {transactions.GetArrayLength()} transactions instead of only coinbase");
    var coinbase = transactions[0];
    if (!coinbase.GetProperty("vin")[0].TryGetProperty("coinbase", out _))
        throw new InvalidOperationException("P2P-fast block's only transaction is not coinbase");

    var expectedPayoutScript = Hex.Encode(
        BitcoinAddress.Create(regtestMiningAddress, Network.RegTest).ScriptPubKey.ToBytes(true));
    JsonElement payout = default;
    var foundPayout = false;
    foreach (var output in coinbase.GetProperty("vout").EnumerateArray())
    {
        var scriptHex = output.GetProperty("scriptPubKey").GetProperty("hex").GetString();
        if (!string.Equals(scriptHex, expectedPayoutScript, StringComparison.OrdinalIgnoreCase))
            continue;
        payout = output;
        foundPayout = true;
        break;
    }
    if (!foundPayout)
        throw new InvalidOperationException("P2P-fast coinbase does not pay the configured address script");
    var expectedPayoutBtc = miningJob.CoinbaseValue / 100_000_000m;
    if (payout.GetProperty("value").GetDecimal() != expectedPayoutBtc)
        throw new InvalidOperationException("P2P-fast coinbase payout amount changed");

    Console.WriteLine(
        $"P2P-fast coinbase-only block height={miningJob.Height} hash={minedHash} " +
        $"txs={transactions.GetArrayLength()} payout_btc={expectedPayoutBtc}");
}

static (string Extranonce2, string Ntime, string Nonce) MineNonBlockStratumShare(
    StratumJob job,
    string extranonce1,
    string extranonce2)
{
    var en2Bytes = Hex.Decode(extranonce2);
    var en1Bytes = Hex.Decode(extranonce1);
    var cb1 = Hex.Decode(job.Coinbase1);
    var cb2 = Hex.Decode(job.Coinbase2);
    var coinbase = new byte[cb1.Length + en1Bytes.Length + en2Bytes.Length + cb2.Length];
    var offset = 0;
    Buffer.BlockCopy(cb1, 0, coinbase, offset, cb1.Length); offset += cb1.Length;
    Buffer.BlockCopy(en1Bytes, 0, coinbase, offset, en1Bytes.Length); offset += en1Bytes.Length;
    Buffer.BlockCopy(en2Bytes, 0, coinbase, offset, en2Bytes.Length); offset += en2Bytes.Length;
    Buffer.BlockCopy(cb2, 0, coinbase, offset, cb2.Length);

    var merkle = BitcoinEncoding.DoubleSha256(coinbase);
    foreach (var branch in job.MerkleBranch)
        merkle = BitcoinEncoding.MerkleStep(merkle, Hex.Decode(branch));

    var prevNotify = Hex.Decode(job.PrevHash);
    var prevBe = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(prevNotify, i * 4, prevBe, (7 - i) * 4, 4);
    var prevLe = Hex.ReverseCopy(prevBe);
    var version = Convert.ToUInt32(job.Version, 16);
    var nbits = Convert.ToUInt32(job.NBits, 16);
    var ntime = Convert.ToUInt32(job.NTime, 16);
    var networkTarget = BitcoinEncoding.CompactTargetToLe(nbits);

    for (uint nonce = 0; nonce < 1_000_000; nonce++)
    {
        var header = BitcoinEncoding.BuildHeader(version, prevLe, merkle, ntime, nbits, nonce);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        if (!BitcoinEncoding.LeqLe256(hashLe, networkTarget))
            return (extranonce2, Hex.U32BeHex(ntime), Hex.U32BeHex(nonce));
    }
    throw new InvalidOperationException("failed to construct a non-block regtest share");
}

static (string Extranonce2, string Ntime, string Nonce) MineTargetSeparatingStratumShare(
    StratumJob job,
    string extranonce1,
    string extranonce2,
    byte[] rejectedTargetLe,
    byte[] acceptedTargetLe)
{
    var en2Bytes = Hex.Decode(extranonce2);
    var en1Bytes = Hex.Decode(extranonce1);
    var cb1 = Hex.Decode(job.Coinbase1);
    var cb2 = Hex.Decode(job.Coinbase2);
    var coinbase = new byte[cb1.Length + en1Bytes.Length + en2Bytes.Length + cb2.Length];
    var offset = 0;
    Buffer.BlockCopy(cb1, 0, coinbase, offset, cb1.Length); offset += cb1.Length;
    Buffer.BlockCopy(en1Bytes, 0, coinbase, offset, en1Bytes.Length); offset += en1Bytes.Length;
    Buffer.BlockCopy(en2Bytes, 0, coinbase, offset, en2Bytes.Length); offset += en2Bytes.Length;
    Buffer.BlockCopy(cb2, 0, coinbase, offset, cb2.Length);

    var merkle = BitcoinEncoding.DoubleSha256(coinbase);
    foreach (var branch in job.MerkleBranch)
        merkle = BitcoinEncoding.MerkleStep(merkle, Hex.Decode(branch));

    var prevNotify = Hex.Decode(job.PrevHash);
    var prevBe = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(prevNotify, i * 4, prevBe, (7 - i) * 4, 4);
    var prevLe = Hex.ReverseCopy(prevBe);
    var version = Convert.ToUInt32(job.Version, 16);
    var nbits = Convert.ToUInt32(job.NBits, 16);
    var ntime = Convert.ToUInt32(job.NTime, 16);
    var networkTargetLe = BitcoinEncoding.CompactTargetToLe(nbits);

    for (uint nonce = 0; nonce < 1_000_000; nonce++)
    {
        var header = BitcoinEncoding.BuildHeader(version, prevLe, merkle, ntime, nbits, nonce);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        if (!BitcoinEncoding.LeqLe256(hashLe, acceptedTargetLe) ||
            BitcoinEncoding.LeqLe256(hashLe, rejectedTargetLe) ||
            BitcoinEncoding.LeqLe256(hashLe, networkTargetLe))
            continue;

        return (extranonce2, Hex.U32BeHex(ntime), Hex.U32BeHex(nonce));
    }

    throw new InvalidOperationException("failed to construct a target-separating Stratum share");
}

static string ToStratumNotifyPrevhash(string blockHashHex)
{
    var blockHashBe = Hex.Decode(blockHashHex);
    var notify = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(blockHashBe, i * 4, notify, (7 - i) * 4, 4);
    return Hex.Encode(notify);
}

static double StopwatchTicksToMilliseconds(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

static async Task RunLargeMempoolPathAsync(
    BitcoinRpcClient rpc,
    string payout,
    string workDir,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    string stratumHost,
    int transactionCount)
{
    var seedTimer = Stopwatch.StartNew();
    var txids = await SeedLargeIndependentMempoolAsync(
        rpc, payout, rpcUrl, rpcUser, rpcPass, transactionCount);
    seedTimer.Stop();

    var mempoolInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    var mempoolSize = mempoolInfo.GetProperty("size").GetInt32();
    if (mempoolSize != transactionCount)
        throw new InvalidOperationException(
            $"large mempool size={mempoolSize}, expected exactly {transactionCount}");

    Console.WriteLine(
        $"large mempool seeded txs={mempoolSize:N0} elapsed={seedTimer.Elapsed.TotalSeconds:F1}s");

    var gatewayDir = Path.Combine(workDir, "large-mempool-gateway");
    var stratumPort = GetFreeTcpPort();
    var apiPort = GetFreeTcpPort();
    var configPath = await WriteRuntimeConfigAsync(
        gatewayDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort);

    var pathTimer = Stopwatch.StartNew();
    var gateway = StartGateway(configPath);
    try
    {
        await WaitHttpAsync(
            $"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(60));
        await RunStratumPathAsync(
            rpc, stratumHost, stratumPort, apiPort,
            expectedTxids: txids,
            requireAllExpectedTxids: true,
            expectedExtranonce2Size: 4,
            expectedTransactionCount: transactionCount,
            gbtTimeout: TimeSpan.FromMinutes(2));
    }
    finally
    {
        TryKill(gateway);
        pathTimer.Stop();
    }

    mempoolInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    var remaining = mempoolInfo.GetProperty("size").GetInt32();
    if (remaining != 0)
        throw new InvalidOperationException(
            $"large-mempool block left {remaining} transactions in mempool");

    var gatewayDataDir = Path.Combine(gatewayDir, "gateway-data");
    var pendingCount = Directory.Exists(Path.Combine(gatewayDataDir, "pending-blocks"))
        ? Directory.GetFiles(Path.Combine(gatewayDataDir, "pending-blocks"), "*.json").Length
        : 0;
    var failedCount = Directory.Exists(Path.Combine(gatewayDataDir, "failed-blocks"))
        ? Directory.GetFiles(Path.Combine(gatewayDataDir, "failed-blocks"), "*.json").Length
        : 0;
    if (pendingCount != 0 || failedCount != 0)
    {
        throw new InvalidOperationException(
            $"large-mempool gateway left pending={pendingCount} failed={failedCount} block files");
    }
    Console.WriteLine(
        $"large mempool published-gateway end-to-end txs={transactionCount:N0} " +
        $"stratum+queue+submit+confirm={pathTimer.Elapsed.TotalSeconds:F1}s " +
        "pending=0 failed=0");
}

static async Task<List<string>> SeedLargeIndependentMempoolAsync(
    BitcoinRpcClient rpc,
    string payout,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    int transactionCount)
{
    const int outputsPerFundingTransaction = 2_000;
    const long fanoutValueSats = 100_000;

    var existing = await rpc.CallAsync<JsonElement>("getrawmempool");
    if (existing.ValueKind == JsonValueKind.Array && existing.GetArrayLength() > 0)
    {
        Console.WriteLine(
            $"clearing existing mempool txs={existing.GetArrayLength():N0} before large seed");
        await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 1, payout });
    }

    var fanoutKey = new Key();
    var fanoutAddress = fanoutKey.PubKey
        .GetAddress(ScriptPubKeyType.Segwit, Network.RegTest);
    var fanoutScript = fanoutAddress.ScriptPubKey;
    var coins = new List<Coin>(transactionCount);
    var fundingCount =
        (transactionCount + outputsPerFundingTransaction - 1) /
        outputsPerFundingTransaction;

    for (var fundingIndex = 0; fundingIndex < fundingCount; fundingIndex++)
    {
        var outputCount = Math.Min(
            outputsPerFundingTransaction, transactionCount - coins.Count);
        var requiredBtc = outputCount * fanoutValueSats / 100_000_000m + 0.01m;
        var unspent = await rpc.CallAsync<JsonElement>(
            "listunspent", new object[] { 1, 9_999_999 });
        var fundingInput = unspent.EnumerateArray().FirstOrDefault(item =>
            (!item.TryGetProperty("spendable", out var spendable) || spendable.GetBoolean()) &&
            item.GetProperty("amount").GetDecimal() >= requiredBtc);
        if (fundingInput.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException(
                $"wallet has no confirmed UTXO large enough for {requiredBtc} BTC fan-out");

        var unsignedFundingTransaction = Network.RegTest.CreateTransaction();
        unsignedFundingTransaction.Version = 2;
        unsignedFundingTransaction.Inputs.Add(new TxIn(new OutPoint(
            uint256.Parse(fundingInput.GetProperty("txid").GetString()),
            fundingInput.GetProperty("vout").GetUInt32())));
        for (var i = 0; i < outputCount; i++)
            unsignedFundingTransaction.Outputs.Add(
                new TxOut(Money.Satoshis(fanoutValueSats), fanoutScript));
        var unsignedHex = unsignedFundingTransaction.ToHex();
        var funded = await rpc.CallAsync<JsonElement>(
            "fundrawtransaction", new object[] { unsignedHex });
        var fundedHex = funded.GetProperty("hex").GetString()
            ?? throw new InvalidOperationException("fundrawtransaction returned no hex");
        var signed = await rpc.CallAsync<JsonElement>(
            "signrawtransactionwithwallet", new object[] { fundedHex });
        if (!signed.GetProperty("complete").GetBoolean())
            throw new InvalidOperationException("wallet did not fully sign fan-out transaction");
        var signedHex = signed.GetProperty("hex").GetString()
            ?? throw new InvalidOperationException("signrawtransactionwithwallet returned no hex");
        var fundingTransaction = Transaction.Parse(signedHex, Network.RegTest);
        var fundingTxid = fundingTransaction.GetHash().ToString();
        var rpcTxid = await rpc.CallAsync<string>(
            "sendrawtransaction", new object[] { signedHex });
        if (!rpcTxid.Equals(fundingTxid, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"fan-out txid mismatch local={fundingTxid} core={rpcTxid}");

        var matched = 0;
        for (var outputIndex = 0; outputIndex < fundingTransaction.Outputs.Count; outputIndex++)
        {
            var output = fundingTransaction.Outputs[outputIndex];
            if (output.Value.Satoshi != fanoutValueSats ||
                !output.ScriptPubKey.Equals(fanoutScript))
                continue;
            coins.Add(new Coin(
                new OutPoint(fundingTransaction.GetHash(), (uint)outputIndex), output));
            matched++;
        }
        if (matched != outputCount)
            throw new InvalidOperationException(
                $"fan-out parent {fundingIndex + 1} has {matched} matching outputs, expected {outputCount}");

        await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 1, payout });
        Console.WriteLine(
            $"confirmed fan-out parent {fundingIndex + 1}/{fundingCount} " +
            $"outputs={outputCount:N0} total_utxos={coins.Count:N0}");
    }

    if (coins.Count != transactionCount)
        throw new InvalidOperationException(
            $"fan-out created {coins.Count} UTXOs, expected {transactionCount}");

    // Four payload bytes keep stripped size above Core's minimum standard
    // transaction size while retaining enough block-weight room for >10k txs.
    var nullDataScript = TxNullDataTemplate.Instance.GenerateScriptPubKey(new byte[4]);
    var secret = fanoutKey.GetBitcoinSecret(Network.RegTest);
    var rawTransactions = new List<string>(transactionCount);
    var txids = new List<string>(transactionCount);
    long rawBytes = 0;
    for (var i = 0; i < coins.Count; i++)
    {
        var transaction = Network.RegTest.CreateTransaction();
        transaction.Version = 2;
        transaction.Inputs.Add(new TxIn(coins[i].Outpoint));
        transaction.Outputs.Add(new TxOut(Money.Zero, nullDataScript));
        transaction.Sign(secret, coins[i]);
        var rawHex = transaction.ToHex();
        rawBytes += rawHex.Length / 2;
        rawTransactions.Add(rawHex);
        txids.Add(transaction.GetHash().ToString());
    }

    Console.WriteLine(
        $"signed independent children txs={transactionCount:N0} " +
        $"raw={rawBytes / 1024d / 1024d:F2} MiB avg={rawBytes / (double)transactionCount:F1} bytes");
    await BroadcastRawTransactionBatchesAsync(
        rpcUrl, rpcUser, rpcPass, rawTransactions, batchSize: 200);

    var deadline = DateTime.UtcNow.AddMinutes(2);
    while (DateTime.UtcNow < deadline)
    {
        var info = await rpc.CallAsync<JsonElement>("getmempoolinfo");
        var size = info.GetProperty("size").GetInt32();
        if (size == transactionCount)
            return txids;
        await Task.Delay(250);
    }

    var finalInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    throw new TimeoutException(
        $"mempool size={finalInfo.GetProperty("size").GetInt32()} did not reach {transactionCount}");
}

static async Task BroadcastRawTransactionBatchesAsync(
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    IReadOnlyList<string> rawTransactions,
    int batchSize)
{
    using var http = new HttpClient
    {
        Timeout = TimeSpan.FromMinutes(5)
    };
    http.DefaultRequestHeaders.Authorization =
        new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{rpcUser}:{rpcPass}")));

    for (var offset = 0; offset < rawTransactions.Count; offset += batchSize)
    {
        var count = Math.Min(batchSize, rawTransactions.Count - offset);
        var requests = new object[count];
        for (var i = 0; i < count; i++)
        {
            requests[i] = new
            {
                jsonrpc = "1.0",
                id = offset + i,
                method = "sendrawtransaction",
                @params = new object[] { rawTransactions[offset + i] }
            };
        }

        using var content = new StringContent(
            JsonSerializer.Serialize(requests), Encoding.UTF8, "application/json");
        using var response = await http.PostAsync(rpcUrl, content);
        response.EnsureSuccessStatusCode();
        await using var responseStream = await response.Content.ReadAsStreamAsync();
        using var document = await JsonDocument.ParseAsync(responseStream);
        if (document.RootElement.ValueKind != JsonValueKind.Array ||
            document.RootElement.GetArrayLength() != count)
            throw new InvalidOperationException(
                $"sendrawtransaction batch response count changed at offset {offset}");

        foreach (var item in document.RootElement.EnumerateArray())
        {
            if (item.TryGetProperty("error", out var error) &&
                error.ValueKind != JsonValueKind.Null)
                throw new InvalidOperationException(
                    $"sendrawtransaction batch failed at offset {offset}: {error}");
            if (!item.TryGetProperty("result", out var result) ||
                result.ValueKind != JsonValueKind.String)
                throw new InvalidOperationException(
                    $"sendrawtransaction batch returned no txid at offset {offset}");
        }

        var broadcast = offset + count;
        if (broadcast == rawTransactions.Count || broadcast % 1_000 == 0)
            Console.WriteLine(
                $"broadcast mempool transactions {broadcast:N0}/{rawTransactions.Count:N0}");
    }
}

/// <summary>
/// Create confirmed UTXOs if needed, then broadcast several bech32→bech32 spends into the mempool.
/// Returns wallet txids that must appear in the next mined block (when fees are high enough for GBT).
/// </summary>
static async Task<List<string>> SeedMempoolAsync(BitcoinRpcClient rpc, int count)
{
    await EnsureSpendableBalanceAsync(rpc, minBtc: count * 0.05 + 1.0);

    // Prefer emptying mempool so GBT is deterministic
    var existing = await rpc.CallAsync<JsonElement>("getrawmempool");
    if (existing.ValueKind == JsonValueKind.Array && existing.GetArrayLength() > 0)
    {
        Console.WriteLine($"mempool already has {existing.GetArrayLength()} tx(s); generating 1 block to clear");
        var sink = await NewBech32AddressAsync(rpc);
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { 1, sink });
    }

    var txids = new List<string>(count);
    for (var i = 0; i < count; i++)
    {
        // Mix address types: bech32 (witness) and p2sh-segwit for broader coverage
        var addrType = i % 2 == 0 ? "bech32" : "p2sh-segwit";
        string dest;
        try
        {
            dest = await rpc.CallAsync<string>("getnewaddress", new object[] { "", addrType });
        }
        catch
        {
            dest = await NewBech32AddressAsync(rpc);
        }

        // Small fixed amount; fallbackfee covers relay
        var amount = 0.01 + i * 0.001;
        var txid = await rpc.CallAsync<string>("sendtoaddress", new object[] { dest, amount });
        txids.Add(txid);
        Console.WriteLine($"seeded mempool tx[{i}] type={addrType} amount={amount} txid={txid}");
    }

    // Wait until mempool reflects broadcasts
    var deadline = DateTime.UtcNow.AddSeconds(10);
    while (DateTime.UtcNow < deadline)
    {
        var mem = await rpc.CallAsync<JsonElement>("getrawmempool");
        var n = mem.ValueKind == JsonValueKind.Array ? mem.GetArrayLength() : 0;
        if (n >= count)
        {
            Console.WriteLine($"mempool size={n} (seeded {count})");
            return txids;
        }
        await Task.Delay(200);
    }

    throw new TimeoutException($"mempool did not reach {count} txs after sendtoaddress");
}

static async Task EnsureSpendableBalanceAsync(BitcoinRpcClient rpc, double minBtc)
{
    for (var attempt = 0; attempt < 6; attempt++)
    {
        double bal;
        try
        {
            bal = await rpc.CallAsync<double>("getbalance", Array.Empty<object>());
        }
        catch
        {
            bal = 0;
        }

        if (bal >= minBtc)
        {
            Console.WriteLine($"wallet balance={bal} BTC (need>={minBtc})");
            return;
        }

        var sink = await NewBech32AddressAsync(rpc);
        Console.WriteLine($"balance {bal} < {minBtc}; mining 50 blocks to {sink}...");
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { 50, sink });
    }

    var finalBal = await rpc.CallAsync<double>("getbalance", Array.Empty<object>());
    if (finalBal < minBtc)
        throw new InvalidOperationException($"insufficient wallet balance {finalBal} BTC (need>={minBtc})");
}

static async Task<string> NewBech32AddressAsync(BitcoinRpcClient rpc)
{
    try
    {
        return await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" });
    }
    catch
    {
        return await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>());
    }
}

static async Task<GbtResponse> WaitForGbtAsync(BitcoinRpcClient rpc, int requireTxCount, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    GbtResponse? last = null;
    while (DateTime.UtcNow < deadline)
    {
        last = await rpc.CallAsync<GbtResponse>("getblocktemplate", new object[]
        {
            new Dictionary<string, object> { ["rules"] = new[] { "segwit" } }
        });
        if (last.TransactionCount >= requireTxCount)
            return last;
        await Task.Delay(200);
    }

    throw new TimeoutException(
        $"GBT txs={last?.TransactionCount ?? 0} < required {requireTxCount} within {timeout.TotalSeconds:0}s");
}

static bool GbtHasWitnessTransactions(GbtResponse gbt)
{
    if (gbt.PackedTransactions != null)
        return gbt.PackedTransactions.HasWitnessTransactions;

    return (gbt.Transactions ?? Array.Empty<GbtTx>()).Any(transaction =>
        transaction.TxId != null && transaction.Hash != null &&
        !transaction.TxId.Equals(transaction.Hash, StringComparison.OrdinalIgnoreCase));
}

static IReadOnlyList<string> GetGbtTxids(GbtResponse gbt)
{
    if (gbt.PackedTransactions == null)
    {
        return (gbt.Transactions ?? Array.Empty<GbtTx>())
            .Select(transaction => transaction.TxId ??
                throw new InvalidOperationException("GBT transaction missing txid"))
            .ToArray();
    }

    var packed = gbt.PackedTransactions;
    var result = new string[packed.Transactions.Count];
    var txidBe = new byte[32];
    for (var txIndex = 0; txIndex < result.Length; txIndex++)
    {
        var sourceOffset = txIndex * 32;
        for (var byteIndex = 0; byteIndex < 32; byteIndex++)
            txidBe[byteIndex] = packed.TxidsLe[sourceOffset + 31 - byteIndex];
        result[txIndex] = Hex.Encode(txidBe);
    }
    return result;
}

static void AssertTransactionsEmbedded(BlockCandidate candidate, TransactionSet transactions)
{
    var block = candidate.Bytes.Span;
    var searchOffset = 0;
    for (var txIndex = 0; txIndex < transactions.Count; txIndex++)
    {
        var transaction = transactions.GetTransaction(txIndex);
        var relativeOffset = block[searchOffset..].IndexOf(transaction);
        if (relativeOffset < 0)
            throw new InvalidOperationException(
                "assembled block bytes missing or reordering a GBT transaction payload");
        searchOffset += relativeOffset + transaction.Length;
    }
}

static void AssertTxidsInBlock(
    IReadOnlyList<string> blockTxids,
    IReadOnlyList<string>? expectedTxids,
    GbtResponse gbt,
    bool requireAllExpectedTxids = false)
{
    var blockTxidSet = new HashSet<string>(
        blockTxids, StringComparer.OrdinalIgnoreCase);

    // Every GBT txid must be in the block (order after coinbase follows template)
    foreach (var id in GetGbtTxids(gbt))
    {
        if (!blockTxidSet.Contains(id))
            throw new InvalidOperationException($"block missing GBT txid {id}");
    }

    if (expectedTxids == null || expectedTxids.Count == 0)
        return;

    var missing = expectedTxids
        .Where(id => !blockTxidSet.Contains(id))
        .ToList();

    // sendtoaddress txids should be selected by GBT when mempool is only our seeds.
    // If Core omitted one (fee policy), require that at least one seeded tx landed AND all GBT txs did.
    if (missing.Count == expectedTxids.Count)
        throw new InvalidOperationException(
            "none of the seeded mempool txids appeared in the mined block");

    if (requireAllExpectedTxids && missing.Count > 0)
        throw new InvalidOperationException(
            $"block missing {missing.Count}/{expectedTxids.Count} required seeded txids: " +
            string.Join(",", missing.Take(5)));
    if (missing.Count > 0)
        Console.WriteLine(
            $"warn: {missing.Count}/{expectedTxids.Count} seeded txids not in block " +
            $"(GBT selected {gbt.TransactionCount} txs): {string.Join(",", missing.Take(5))}");
    else
        Console.WriteLine($"all {expectedTxids.Count} seeded mempool txids confirmed in block");
}

static ShareResult? MineBlock(JobTemplate job, byte[] coinbasePrefix, string extranonce2, byte[] shareTargetLe, int maxNonces)
{
    var ntime = job.Ntime;
    if (job.Mintime != 0 && ntime < job.Mintime)
        ntime = job.Mintime;

    for (uint nonce = 0; nonce < (uint)maxNonces; nonce++)
    {
        var result = ShareValidator.Validate(job, coinbasePrefix, new ShareSubmit
        {
            Extranonce2 = extranonce2,
            Ntime = Hex.U32BeHex(ntime),
            Nonce = Hex.U32BeHex(nonce)
        }, shareTargetLe);

        if (result.IsBlock)
            return result;
    }

    return null;
}

static (string Extranonce2, string Ntime, string Nonce, string HashHex)? MineStratumJob(
    StratumJob job,
    string extranonce1,
    int maxNonces,
    string? extranonce2 = null)
{
    // Reconstruct coinbase and header the same way the gateway does.
    // We only need a valid PoW header; submit uses en2/ntime/nonce.
    var en2 = extranonce2 ?? "00000001";
    var en2Bytes = Hex.Decode(en2);
    var en1Bytes = Hex.Decode(extranonce1);
    var cb1 = Hex.Decode(job.Coinbase1);
    var cb2 = Hex.Decode(job.Coinbase2);

    var coinbase = new byte[cb1.Length + en1Bytes.Length + en2Bytes.Length + cb2.Length];
    var o = 0;
    Buffer.BlockCopy(cb1, 0, coinbase, o, cb1.Length); o += cb1.Length;
    Buffer.BlockCopy(en1Bytes, 0, coinbase, o, en1Bytes.Length); o += en1Bytes.Length;
    Buffer.BlockCopy(en2Bytes, 0, coinbase, o, en2Bytes.Length); o += en2Bytes.Length;
    Buffer.BlockCopy(cb2, 0, coinbase, o, cb2.Length);

    var coinbaseHash = BitcoinEncoding.DoubleSha256(coinbase);
    var merkle = coinbaseHash;
    foreach (var br in job.MerkleBranch)
    {
        var b = Hex.Decode(br);
        merkle = BitcoinEncoding.MerkleStep(merkle, b);
    }

    // prevhash in notify is word-swapped BE; convert to LE for header
    var prevNotify = Hex.Decode(job.PrevHash);
    var prevBe = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(prevNotify, i * 4, prevBe, (7 - i) * 4, 4);
    var prevLe = Hex.ReverseCopy(prevBe);

    var version = Convert.ToUInt32(job.Version, 16);
    var nbits = Convert.ToUInt32(job.NBits, 16);
    var ntime = Convert.ToUInt32(job.NTime, 16);
    var targetLe = BitcoinEncoding.CompactTargetToLe(nbits);

    for (uint nonce = 0; nonce < (uint)maxNonces; nonce++)
    {
        var header = BitcoinEncoding.BuildHeader(version, prevLe, merkle, ntime, nbits, nonce);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        if (!BitcoinEncoding.LeqLe256(hashLe, targetLe))
            continue;

        var hashHex = Hex.Encode(Hex.ReverseCopy(hashLe));
        return (en2, Hex.U32BeHex(ntime), Hex.U32BeHex(nonce), hashHex);
    }

    return null;
}

static AppConfig BuildConfig(
    string payout,
    string rpcUrl,
    string user,
    string pass,
    int extranonce1Size = 4,
    int extranonce2Size = 4)
{
    var cfg = new AppConfig
    {
        NetworkName = "regtest",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = rpcUrl,
            RpcUser = user,
            RpcPassword = pass
        },
        Coinbase = new CoinbaseConfig
        {
            Address = payout,
            Message = "regtest-e2e",
            SegwitCommitment = true
        },
        Stratum = new StratumConfig
        {
            Extranonce1Size = extranonce1Size,
            Extranonce2Size = extranonce2Size,
            MaxConnections = 32
        },
        Difficulty = new DifficultyConfig { Min = 0.001, Max = 1, Default = 0.01 }
    };
    cfg.Validate();
    return cfg;
}

static async Task<string> WriteRuntimeConfigAsync(
    string workDir, string payout, string rpcUrl, string user, string pass, int stratumPort, int apiPort,
    bool lifecycleMode = false,
    int extranonce1Size = 4,
    int extranonce2Size = 4)
{
    var path = Path.Combine(workDir, "gateway.json");
    var cfg = new AppConfig
    {
        LogLevel = "Information",
        NetworkName = "regtest",
        Stratum = new StratumConfig
        {
            ListenAddr = "127.0.0.1",
            ListenPort = stratumPort,
            Extranonce1Size = extranonce1Size,
            Extranonce2Size = extranonce2Size,
            IdleTimeoutSecs = 3600,
            MaxConnections = 32
        },
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = rpcUrl,
            RpcUser = user,
            RpcPassword = pass,
            ZmqBlockUrls = new List<string> { "tcp://127.0.0.1:28332" },
            P2pFastPeer = "127.0.0.1:18444"
        },
        Coinbase = new CoinbaseConfig
        {
            Address = payout,
            Message = "regtest-e2e",
            SegwitCommitment = true
        },
        Difficulty = new DifficultyConfig
        {
            Min = lifecycleMode ? 1e-12 : 3e-10,
            Max = lifecycleMode ? 1e-6 : 1,
            Default = lifecycleMode ? 1e-12 : 0.01,
            TargetTimeSecs = 5,
            RetargetTimeSecs = 90
        },
        Runtime = new RuntimeConfig
        {
            KeepOldJobs = 8,
            MaxRetiredJobs = lifecycleMode ? 64 : 8,
            DataDir = Path.Combine(workDir, "gateway-data")
        },
        Api = new ApiConfig
        {
            ListenAddr = "127.0.0.1",
            ListenPort = apiPort,
            Enabled = true
        }
    };
    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
    await File.WriteAllTextAsync(path, JsonSerializer.Serialize(cfg, AppConfig.JsonOptions()));
    return path;
}

static Process StartGateway(
    string configPath,
    ConcurrentQueue<string>? capturedLogs = null)
{
    var repo = FindRepoRoot();
    var targetFramework = $"net{Environment.Version.Major}.0";
    var candidates = new[]
    {
        Path.Combine(repo, "build", "MiningcoreBtcSolo.exe"),
        Path.Combine(repo, "build", "MiningcoreBtcSolo"),
        Path.Combine(repo, "src", "MiningcoreBtcSolo", "bin", "Release", targetFramework, "MiningcoreBtcSolo.exe"),
        Path.Combine(repo, "src", "MiningcoreBtcSolo", "bin", "Debug", targetFramework, "MiningcoreBtcSolo.exe")
    };
    var exe = candidates.FirstOrDefault(File.Exists);
    ProcessStartInfo psi;
    if (exe != null)
    {
        psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = $"--config \"{configPath}\"",
            WorkingDirectory = Path.GetDirectoryName(exe)!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
    }
    else
    {
        var csproj = Path.Combine(repo, "src", "MiningcoreBtcSolo", "MiningcoreBtcSolo.csproj");
        psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project \"{csproj}\" -c Release --no-build -- --config \"{configPath}\"",
            WorkingDirectory = repo,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
    }

    var p = Process.Start(psi) ?? throw new InvalidOperationException("failed to start gateway");
    p.OutputDataReceived += (_, e) =>
    {
        if (e.Data == null) return;
        capturedLogs?.Enqueue(e.Data);
        Console.WriteLine($"[gateway] {e.Data}");
    };
    p.ErrorDataReceived += (_, e) =>
    {
        if (e.Data == null) return;
        capturedLogs?.Enqueue(e.Data);
        Console.Error.WriteLine($"[gateway] {e.Data}");
    };
    p.BeginOutputReadLine();
    p.BeginErrorReadLine();
    return p;
}

static void EnsureBitcoind(ref Process? bitcoind, string datadir, string rpcUrl, string user, string pass)
{
    // Probe RPC first
    try
    {
        var probe = new BitcoinRpcClient(rpcUrl, user, pass);
        probe.CallAsync<JsonElement>("getblockchaininfo").GetAwaiter().GetResult();
        Console.WriteLine("bitcoind RPC already up");
        return;
    }
    catch
    {
        // start local
    }

    Directory.CreateDirectory(datadir);
    bitcoind = StartOwnedBitcoind(datadir, rpcUrl, user, pass);
}

static Process StartOwnedBitcoind(
    string datadir,
    string rpcUrl,
    string user,
    string pass)
{
    var rpcUri = new Uri(rpcUrl, UriKind.Absolute);
    if (rpcUri.Scheme != Uri.UriSchemeHttp ||
        !IPAddress.TryParse(rpcUri.Host, out var rpcAddress) ||
        !IPAddress.IsLoopback(rpcAddress))
    {
        throw new InvalidOperationException(
            $"owned regtest Core requires a loopback HTTP RPC URL, got {rpcUrl}");
    }

    Directory.CreateDirectory(datadir);
    var bitcoindExe = FindBitcoind();
    Console.WriteLine($"starting owned bitcoind: {bitcoindExe}");
    var startInfo = new ProcessStartInfo
    {
        FileName = bitcoindExe,
        UseShellExecute = false,
        RedirectStandardOutput = true,
        RedirectStandardError = true
    };
    foreach (var argument in new[]
             {
                 $"-datadir={datadir}",
                 "-regtest=1",
                 "-server=1",
                 "-txindex=1",
                 $"-rpcuser={user}",
                 $"-rpcpassword={pass}",
                 $"-rpcport={rpcUri.Port}",
                 $"-rpcallowip={rpcAddress}",
                 $"-rpcbind={rpcAddress}:{rpcUri.Port}",
                 "-bind=127.0.0.1:18444",
                 "-discover=0",
                 "-dnsseed=0",
                 "-fixedseeds=0",
                 "-natpmp=0",
                 "-fallbackfee=0.0002",
                 "-acceptnonstdtxn=1",
                 "-zmqpubhashblock=tcp://127.0.0.1:28332",
                 "-zmqpubrawblock=tcp://127.0.0.1:28333"
             })
    {
        startInfo.ArgumentList.Add(argument);
    }

    var process = Process.Start(startInfo)
        ?? throw new InvalidOperationException("failed to start bitcoind");

    try
    {
        File.WriteAllText(
            OwnedBitcoindMarkerPath(datadir),
            JsonSerializer.Serialize(new
            {
                pid = process.Id,
                started_at_unix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                chain = "regtest"
            }));
    }
    catch
    {
        TryKill(process);
        throw;
    }

    process.OutputDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"[bitcoind] {e.Data}"); };
    process.ErrorDataReceived += (_, e) => { if (e.Data != null) Console.Error.WriteLine($"[bitcoind] {e.Data}"); };
    process.BeginOutputReadLine();
    process.BeginErrorReadLine();
    return process;
}

static string OwnedBitcoindMarkerPath(string datadir) =>
    Path.Combine(datadir, ".miningcore-regtest-owned.json");

static void AssertOwnedBitcoind(
    Process process,
    string workDir,
    string datadir,
    string rpcUrl)
{
    if (process.HasExited)
        throw new InvalidOperationException("owned bitcoind process has already exited");

    var root = Path.GetFullPath(workDir).TrimEnd(
        Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
    var candidate = Path.GetFullPath(datadir).TrimEnd(
        Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
    if (!candidate.StartsWith(root, StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException(
            $"refusing to stop Core outside this harness work directory: datadir={candidate} root={root}");
    }

    var rpcUri = new Uri(rpcUrl, UriKind.Absolute);
    if (rpcUri.Scheme != Uri.UriSchemeHttp ||
        !IPAddress.TryParse(rpcUri.Host, out var rpcAddress) ||
        !IPAddress.IsLoopback(rpcAddress))
        throw new InvalidOperationException($"refusing to stop Core with non-loopback RPC URL: {rpcUrl}");

    var markerPath = OwnedBitcoindMarkerPath(datadir);
    if (!File.Exists(markerPath))
        throw new InvalidOperationException($"owned Core marker is missing: {markerPath}");
    using var marker = JsonDocument.Parse(File.ReadAllText(markerPath));
    if (!marker.RootElement.TryGetProperty("pid", out var pidElement) ||
        !pidElement.TryGetInt32(out var markerPid) || markerPid != process.Id ||
        marker.RootElement.GetProperty("chain").GetString() != "regtest")
    {
        throw new InvalidOperationException(
            $"owned Core marker does not match process pid={process.Id}");
    }
}

static async Task StopOwnedBitcoindGracefullyAsync(
    Process process,
    string workDir,
    string datadir,
    string rpcUrl,
    BitcoinRpcClient rpc)
{
    AssertOwnedBitcoind(process, workDir, datadir, rpcUrl);
    try
    {
        _ = await rpc.CallAsync<JsonElement>("stop");
    }
    catch (Exception ex)
    {
        // Core may close the HTTP connection immediately after accepting stop.
        Console.WriteLine($"owned Core stop RPC ended with {ex.GetType().Name}: {ex.Message}");
    }

    var deadline = DateTime.UtcNow.AddSeconds(30);
    while (!process.HasExited && DateTime.UtcNow < deadline)
        await Task.Delay(100);
    if (process.HasExited)
        return;

    TryKill(process);
    throw new TimeoutException(
        $"owned bitcoind pid={process.Id} did not complete graceful shutdown");
}

static string FindBitcoind()
{
    var env = Environment.GetEnvironmentVariable("BITCOIND");
    if (!string.IsNullOrWhiteSpace(env) && File.Exists(env))
        return env;

    var pf = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
    var candidates = new[]
    {
        Path.Combine(pf, "Bitcoin", "daemon", "bitcoind.exe"),
        Path.Combine(pf, "Bitcoin", "bitcoind.exe"),
        "bitcoind"
    };
    foreach (var c in candidates)
    {
        if (c == "bitcoind")
            return c;
        if (File.Exists(c))
            return c;
    }
    throw new FileNotFoundException("bitcoind not found. Install Bitcoin Core or set BITCOIND.");
}

static void WaitForRpc(BitcoinRpcClient rpc, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    Exception? last = null;
    while (DateTime.UtcNow < deadline)
    {
        try
        {
            rpc.CallAsync<JsonElement>("getblockchaininfo").GetAwaiter().GetResult();
            Console.WriteLine("RPC ready");
            return;
        }
        catch (Exception ex)
        {
            last = ex;
            Thread.Sleep(500);
        }
    }
    throw new TimeoutException($"RPC not ready: {last?.Message}");
}

static async Task VerifyHarnessChainIdentityAsync(BitcoinRpcClient rpc)
{
    var info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    if (!info.TryGetProperty("chain", out var chainElement) ||
        chainElement.ValueKind != JsonValueKind.String ||
        !string.Equals(chainElement.GetString(), "regtest", StringComparison.OrdinalIgnoreCase))
    {
        var actual = chainElement.ValueKind == JsonValueKind.String
            ? chainElement.GetString()
            : chainElement.ValueKind.ToString();
        throw new InvalidOperationException(
            $"regtest harness requires getblockchaininfo.chain=regtest, got {actual}");
    }

    Console.WriteLine("PASS harness chain identity (regtest)");
}

static async Task WaitHttpAsync(string url, TimeSpan timeout)
{
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    var deadline = DateTime.UtcNow + timeout;
    Exception? last = null;
    while (DateTime.UtcNow < deadline)
    {
        try
        {
            var resp = await http.GetAsync(url);
            if (resp.IsSuccessStatusCode)
            {
                Console.WriteLine($"HTTP ready {url}");
                return;
            }
        }
        catch (Exception ex)
        {
            last = ex;
        }
        await Task.Delay(400);
    }
    throw new TimeoutException($"HTTP not ready {url}: {last?.Message}");
}

static async Task<JsonElement> WaitForPublishedGatewayJobAsync(
    int apiPort,
    int requireTxCount,
    uint expectedHeight,
    TimeSpan timeout)
{
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    var url = $"http://127.0.0.1:{apiPort}/api/stats";
    var deadline = DateTime.UtcNow + timeout;
    JsonElement lastStats = default;
    Exception? lastError = null;

    while (DateTime.UtcNow < deadline)
    {
        try
        {
            using var response = await http.GetAsync(url);
            var payload = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                using var document = JsonDocument.Parse(payload);
                lastStats = document.RootElement.Clone();
                var ready = lastStats.TryGetProperty("ready", out var readyElement) &&
                    readyElement.ValueKind is JsonValueKind.True;
                var height = lastStats.TryGetProperty("current_height", out var heightElement) &&
                    heightElement.TryGetUInt32(out var parsedHeight)
                    ? parsedHeight
                    : 0U;
                var transactionCount = lastStats.TryGetProperty("txn_count", out var txElement) &&
                    txElement.TryGetInt32(out var parsedTransactionCount)
                    ? parsedTransactionCount
                    : 0;

                if (ready && height == expectedHeight && transactionCount >= requireTxCount)
                {
                    Console.WriteLine(
                        $"published gateway job ready height={height} txs={transactionCount}");
                    return lastStats;
                }
            }
        }
        catch (Exception ex)
        {
            lastError = ex;
        }

        await Task.Delay(200);
    }

    var detail = lastStats.ValueKind == JsonValueKind.Object
        ? lastStats.ToString()
        : lastError?.Message ?? "no stats response";
    throw new TimeoutException(
        $"published gateway job did not reach ready height={expectedHeight} " +
        $"txs>={requireTxCount} within {timeout.TotalSeconds:0}s: {detail}");
}

static async Task<string> EnsureWalletAndPayoutAsync(BitcoinRpcClient rpc)
{
    const string wallet = "regtest_solo";

    // Load or create named wallet (descriptor wallets are default on Core 26+)
    var loaded = false;
    try
    {
        var wallets = await rpc.CallAsync<JsonElement>("listwallets", Array.Empty<object>());
        if (wallets.ValueKind == JsonValueKind.Array &&
            wallets.EnumerateArray().Any(w => w.GetString() == wallet))
            loaded = true;
    }
    catch { /* ignore */ }

    if (!loaded)
    {
        try
        {
            await rpc.CallAsync<JsonElement>("loadwallet", new object[] { wallet });
            loaded = true;
            Console.WriteLine($"loaded wallet {wallet}");
        }
        catch (Exception loadEx)
        {
            try
            {
                await rpc.CallAsync<JsonElement>("createwallet", new object[] { wallet });
                loaded = true;
                Console.WriteLine($"created wallet {wallet}");
            }
            catch (Exception createEx)
            {
                // Already exists on disk but not loaded, or race — try load once more
                try
                {
                    await rpc.CallAsync<JsonElement>("loadwallet", new object[] { wallet });
                    loaded = true;
                    Console.WriteLine($"loaded wallet {wallet} after create conflict");
                }
                catch
                {
                    throw new InvalidOperationException(
                        $"unable to create/load wallet '{wallet}': load={loadEx.Message}; create={createEx.Message}");
                }
            }
        }
    }

    if (!loaded)
        throw new InvalidOperationException($"wallet '{wallet}' not available");

    // Prefer bech32 (mainnet-like coinbase script)
    string address;
    try
    {
        address = await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" });
    }
    catch
    {
        address = await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>());
    }

    // Validate with NBitcoin RegTest
    _ = BitcoinAddress.Create(address, Network.RegTest);
    return address;
}

static async Task EnsureChainReadyAsync(BitcoinRpcClient rpc)
{
    var info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var blocks = info.GetProperty("blocks").GetInt32();
    if (blocks < 101)
    {
        // generate to a disposable address so GBT height >= 101 (coinbase maturity)
        string sink;
        try { sink = await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" }); }
        catch { sink = await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>()); }

        var need = 101 - blocks;
        Console.WriteLine($"generatetoaddress {need} blocks for mature chain...");
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { need, sink });
    }

    // One more empty tip so our mined block is cleanly next
    info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    Console.WriteLine($"chain ready height={info.GetProperty("blocks").GetInt32()} chain={info.GetProperty("chain").GetString()}");
}

static void PrintChecklist(List<string> passed, List<string> failures)
{
    var items = new (string key, bool ok)[]
    {
        ("Regtest empty-block path (GBT → submitblock → tip)", passed.Any(p => p.Contains("empty"))),
        ("Mempool multi-tx GBT (witness txs + commitment)", passed.Any(p => p.Contains("mempool"))),
        ("Multi-tx full block assembly + submitblock accepted", passed.Any(p => p.Contains("mempool"))),
        ("Seeded mempool txids present in active-chain block", passed.Any(p => p.Contains("mempool"))),
        ("Stratum V1 path with multi-tx template", passed.Any(p => p.Contains("stratum mempool"))),
        ("Gateway submitblock after mining.submit (multi-tx)", passed.Any(p => p.Contains("stratum mempool"))),
        ("P2P-fast coinbase-only job mined into the active chain",
            passed.Any(p => p.Contains("p2p-fast coinbase-only"))),
        ("Rapid clean fan-out + concurrent late shares", passed.Any(p => p.Contains("clean lifecycle"))),
        ("Extranonce1 1-4 x extranonce2 1-8 share+block",
            passed.Any(p => p.Contains("extranonce", StringComparison.Ordinal)))
    };

    foreach (var (key, ok) in items)
        Console.WriteLine($"  [{(ok ? "x" : " ")}] {key}");

    Console.WriteLine();
    Console.WriteLine("Before mainnet go-live, still verify manually:");
    Console.WriteLine("  [ ] config.json network=mainnet");
    Console.WriteLine("  [ ] coinbase.address is YOUR mainnet payout (bech32 recommended)");
    Console.WriteLine("  [ ] bitcoind RPC auth + firewall (no public RPC)");
    Console.WriteLine("  [ ] zmq_block_urls / p2p_fast_peer(s) point at your node(s)");
    Console.WriteLine("  [ ] difficulty.min/default sized for your hashrate");
    Console.WriteLine("  [ ] /healthz and /readyz monitored");
    Console.WriteLine("  [ ] sole miner points at this gateway (not a pool)");

    if (failures.Count > 0)
        Console.WriteLine("FAILED items: " + string.Join(" | ", failures));
}

static void TryKill(Process? p)
{
    if (p == null) return;
    try
    {
        if (!p.HasExited)
        {
            p.Kill(entireProcessTree: true);
            p.WaitForExit(5000);
        }
    }
    catch { /* ignore */ }
}

static string FindRepoRoot()
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir != null)
    {
        if (File.Exists(Path.Combine(dir.FullName, "MiningcoreBtcSolo.sln")))
            return dir.FullName;
        if (File.Exists(Path.Combine(dir.FullName, "config.json")) &&
            Directory.Exists(Path.Combine(dir.FullName, "src")))
            return dir.FullName;
        dir = dir.Parent;
    }
    return Directory.GetCurrentDirectory();
}

static string Env(string key, string fallback)
    => Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

// ---------------------------------------------------------------------------
// Minimal Stratum V1 client
// ---------------------------------------------------------------------------

sealed class StratumMinerClient : IDisposable
{
    private TcpClient? _tcp;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private int _id;
    private readonly Dictionary<int, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly object _gate = new();
    private CancellationTokenSource? _cts;
    private readonly Channel<StratumJob> _jobs = Channel.CreateUnbounded<StratumJob>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly Channel<double> _difficulties = Channel.CreateUnbounded<double>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly Channel<string> _versionMasks = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private int _notificationCount;
    private int _cleanNotificationCount;
    private int _difficultyNotificationCount;

    public int NotificationCount => Volatile.Read(ref _notificationCount);
    public int CleanNotificationCount => Volatile.Read(ref _cleanNotificationCount);
    public int DifficultyNotificationCount => Volatile.Read(ref _difficultyNotificationCount);

    public async Task ConnectAsync(string host, int port)
    {
        _tcp = new TcpClient();
        await _tcp.ConnectAsync(host, port);
        _tcp.NoDelay = true;
        var stream = _tcp.GetStream();
        _reader = new StreamReader(stream, Encoding.UTF8);
        _writer = new StreamWriter(stream, new UTF8Encoding(false)) { NewLine = "\n", AutoFlush = true };
        _cts = new CancellationTokenSource();
        _ = Task.Run(() => ReadLoop(_cts.Token));
    }

    public async Task<(string Extranonce1, int Extranonce2Size)> SubscribeAsync(string ua)
    {
        var result = await RequestAsync("mining.subscribe", new object[] { ua });
        // [[notifications], extranonce1, extranonce2_size]
        var en1 = result[1].GetString() ?? throw new InvalidOperationException("no extranonce1");
        var en2Size = result[2].GetInt32();
        return (en1, en2Size);
    }

    public async Task AuthorizeAsync(string user, string pass)
    {
        var result = await RequestAsync("mining.authorize", new object[] { user, pass });
        if (result.ValueKind == JsonValueKind.False)
            throw new InvalidOperationException("authorize rejected");
    }

    public async Task SuggestDifficultyAsync(double difficulty)
    {
        var result = await RequestAsync("mining.suggest_difficulty", new object[] { difficulty });
        if (result.ValueKind == JsonValueKind.False)
            throw new InvalidOperationException("suggest_difficulty rejected");
    }

    public async Task<string> ConfigureVersionRollingAsync(
        string requestedMask, bool includeMinBitCount = true)
    {
        var parameters = new Dictionary<string, object>
        {
            ["version-rolling.mask"] = requestedMask
        };
        if (includeMinBitCount)
            parameters["version-rolling.min-bit-count"] = 2;

        var result = await RequestAsync("mining.configure", new object[]
        {
            new[] { "version-rolling" },
            parameters
        });
        if (result.ValueKind != JsonValueKind.Object ||
            !result.TryGetProperty("version-rolling", out var enabled) ||
            enabled.ValueKind != JsonValueKind.True ||
            !result.TryGetProperty("version-rolling.mask", out var mask))
        {
            throw new InvalidOperationException($"version-rolling negotiation failed: {result}");
        }
        return mask.GetString() ?? throw new InvalidOperationException("version-rolling mask missing");
    }

    public async Task ConfigureWithoutExtensionsAsync()
    {
        await RequestAsync("mining.configure", new object[]
        {
            Array.Empty<string>(),
            new Dictionary<string, string>()
        });
    }

    public async Task<StratumJob> WaitForJobAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        return await _jobs.Reader.ReadAsync(cts.Token);
    }

    public async Task<StratumJob> WaitForJobAsync(Func<StratumJob, bool> predicate, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (await _jobs.Reader.WaitToReadAsync(cts.Token))
        {
            while (_jobs.Reader.TryRead(out var job))
            {
                if (predicate(job))
                    return job;
            }
        }
        throw new TimeoutException("mining.notify");
    }

    public async Task<double> WaitForDifficultyAsync(TimeSpan timeout)
        => await WaitForDifficultyAsync(_ => true, timeout);

    public async Task<double> WaitForDifficultyAsync(
        Func<double, bool> predicate,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (await _difficulties.Reader.WaitToReadAsync(cts.Token))
        {
            while (_difficulties.Reader.TryRead(out var difficulty))
            {
                if (predicate(difficulty))
                    return difficulty;
            }
        }
        throw new TimeoutException("mining.set_difficulty");
    }

    public async Task<string> WaitForVersionMaskAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        return await _versionMasks.Reader.ReadAsync(cts.Token);
    }

    public async Task<bool> SubmitAsync(
        string worker,
        string jobId,
        string en2,
        string ntime,
        string nonce,
        string? versionBits = null)
    {
        object[] parameters = versionBits == null
            ? new object[] { worker, jobId, en2, ntime, nonce }
            : new object[] { worker, jobId, en2, ntime, nonce, versionBits };
        var result = await RequestAsync("mining.submit", parameters);
        return result.ValueKind != JsonValueKind.False;
    }

    public async Task<string> SubmitExpectErrorAsync(
        string worker,
        string jobId,
        string en2,
        string ntime,
        string nonce,
        string? versionBits = null)
    {
        try
        {
            await SubmitAsync(worker, jobId, en2, ntime, nonce, versionBits);
            throw new InvalidOperationException("mining.submit unexpectedly succeeded");
        }
        catch (InvalidOperationException ex) when (!ex.Message.Contains(
                   "unexpectedly succeeded", StringComparison.Ordinal))
        {
            return ex.Message;
        }
    }

    private async Task<JsonElement> RequestAsync(string method, object[] parameters)
    {
        var id = Interlocked.Increment(ref _id);
        var tcs = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate) _pending[id] = tcs;

        var payload = JsonSerializer.Serialize(new
        {
            id,
            method,
            @params = parameters
        });
        await _writer!.WriteLineAsync(payload);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var reg = cts.Token.Register(() => tcs.TrySetException(new TimeoutException(method)));
        return await tcs.Task;
    }

    private async Task ReadLoop(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var line = await _reader!.ReadLineAsync(ct);
                if (line == null) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                if (root.TryGetProperty("method", out var methodEl))
                {
                    var method = methodEl.GetString();
                    var p = root.GetProperty("params");
                    if (method == "mining.set_difficulty")
                    {
                        Interlocked.Increment(ref _difficultyNotificationCount);
                        _difficulties.Writer.TryWrite(p[0].GetDouble());
                    }
                    else if (method == "mining.set_version_mask")
                    {
                        _versionMasks.Writer.TryWrite(p[0].GetString()!);
                    }
                    else if (method == "mining.notify")
                    {
                        var job = new StratumJob
                        {
                            JobId = p[0].GetString()!,
                            PrevHash = p[1].GetString()!,
                            Coinbase1 = p[2].GetString()!,
                            Coinbase2 = p[3].GetString()!,
                            MerkleBranch = p[4].EnumerateArray().Select(x => x.GetString()!).ToList(),
                            Version = p[5].GetString()!,
                            NBits = p[6].GetString()!,
                            NTime = p[7].GetString()!,
                            CleanJobs = p.GetArrayLength() > 8 && p[8].GetBoolean(),
                            ReceivedTimestamp = Stopwatch.GetTimestamp()
                        };
                        Interlocked.Increment(ref _notificationCount);
                        if (job.CleanJobs)
                            Interlocked.Increment(ref _cleanNotificationCount);
                        _jobs.Writer.TryWrite(job);
                    }
                    continue;
                }

                if (root.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.Number)
                {
                    var id = idEl.GetInt32();
                    TaskCompletionSource<JsonElement>? tcs;
                    lock (_gate) _pending.Remove(id, out tcs);
                    if (tcs == null) continue;

                    if (root.TryGetProperty("error", out var err) && err.ValueKind != JsonValueKind.Null)
                    {
                        tcs.TrySetException(new InvalidOperationException($"stratum error: {err}"));
                    }
                    else if (root.TryGetProperty("result", out var result))
                    {
                        tcs.TrySetResult(result.Clone());
                    }
                    else
                    {
                        tcs.TrySetException(new InvalidOperationException("stratum response missing result"));
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"stratum read loop: {ex.Message}");
        }
    }

    public void Dispose()
    {
        try { _cts?.Cancel(); } catch { }
        try { _tcp?.Close(); } catch { }
        _reader?.Dispose();
        _writer?.Dispose();
        _tcp?.Dispose();
        _cts?.Dispose();
    }
}

sealed class StratumJob
{
    public string JobId { get; init; } = "";
    public string PrevHash { get; init; } = "";
    public string Coinbase1 { get; init; } = "";
    public string Coinbase2 { get; init; } = "";
    public List<string> MerkleBranch { get; init; } = new();
    public string Version { get; init; } = "";
    public string NBits { get; init; } = "";
    public string NTime { get; init; } = "";
    public bool CleanJobs { get; init; }
    public long ReceivedTimestamp { get; init; }
}

sealed class TestSequenceSegment : ReadOnlySequenceSegment<byte>
{
    private TestSequenceSegment(ReadOnlyMemory<byte> memory)
    {
        Memory = memory;
    }

    public static ReadOnlySequence<byte> Create(params ReadOnlyMemory<byte>[] segments)
    {
        if (segments.Length == 0)
            return ReadOnlySequence<byte>.Empty;

        var first = new TestSequenceSegment(segments[0]);
        var last = first;
        for (var i = 1; i < segments.Length; i++)
        {
            var next = new TestSequenceSegment(segments[i])
            {
                RunningIndex = last.RunningIndex + last.Memory.Length
            };
            last.Next = next;
            last = next;
        }
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }
}
