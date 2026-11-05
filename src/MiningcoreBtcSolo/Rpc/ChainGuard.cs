using System.Text.Json;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Rpc;

/// <summary>
/// Fail-fast check that bitcoind's chain matches config.network before any mining work.
/// </summary>
public static class ChainGuard
{
    public static async Task VerifyAsync(AppConfig cfg, BitcoinRpcClient rpc, CancellationToken ct)
    {
        var expected = ExpectedBitcoindChain(cfg.NetworkName);
        JsonElement info;
        try
        {
            info = await rpc.CallAsync<JsonElement>("getblockchaininfo", Array.Empty<object>(), ct);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"getblockchaininfo failed (is bitcoind RPC reachable at {cfg.Bitcoind.RpcUrl}?): {ex.Message}", ex);
        }

        if (!info.TryGetProperty("chain", out var chainEl) || chainEl.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("getblockchaininfo missing string field 'chain'");

        var chain = chainEl.GetString() ?? "";
        if (!string.Equals(chain, expected, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"bitcoind chain mismatch: config.network={cfg.NetworkName} expects chain={expected}, " +
                $"getblockchaininfo.chain={chain}. Refusing to start (would mine on the wrong network).");
        }

        var blocks = info.TryGetProperty("blocks", out var b) && b.TryGetInt32(out var bi) ? bi : -1;
        var headers = info.TryGetProperty("headers", out var h) && h.TryGetInt32(out var hi) ? hi : -1;
        if (!info.TryGetProperty("initialblockdownload", out var ibdEl) ||
            ibdEl.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
        {
            throw new InvalidOperationException(
                "getblockchaininfo missing boolean field 'initialblockdownload'");
        }
        var ibd = ibdEl.GetBoolean();

        SoloLog.Info("bitcoind chain verified",
            ("config_network", cfg.NetworkName),
            ("chain", chain),
            ("blocks", blocks),
            ("headers", headers),
            ("ibd", ibd));

        EnsureMiningReady(ibd, blocks, headers);
        if (blocks < headers)
        {
            SoloLog.Warn("bitcoind headers ahead of blocks (node not fully synced)",
                ("blocks", blocks),
                ("headers", headers));
        }
    }

    internal static void EnsureMiningReady(bool initialBlockDownload, int blocks, int headers)
    {
        if (blocks < 0 || headers < 0)
            throw new InvalidOperationException(
                "getblockchaininfo returned invalid blocks/headers counters");
        if (initialBlockDownload)
        {
            throw new InvalidOperationException(
                $"bitcoind is still in initial block download (IBD): blocks={blocks} headers={headers}. " +
                "Refusing to start until synchronization completes.");
        }
    }

    /// <summary>Maps config.network to Bitcoin Core getblockchaininfo.chain values.</summary>
    public static string ExpectedBitcoindChain(string networkName) => networkName.ToLowerInvariant() switch
    {
        "mainnet" or "bitcoin" => "main",
        "testnet" => "test",
        "regtest" => "regtest",
        _ => throw new InvalidOperationException($"Unsupported network for chain check: {networkName}")
    };
}
