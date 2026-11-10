using System.Security.Cryptography;
using System.Globalization;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Util;
using NBitcoin;

namespace MiningcoreBtcSolo.Template;

/// <summary>
/// Builds stratum jobs and full blocks. Coinbase always pays the fixed configured address
/// (true solo — no wallet payout scheme).
/// </summary>
public sealed class JobBuilder
{
    private const long MaxMoneySatoshis = 21_000_000L * 100_000_000L;
    private const string WitnessCommitmentPrefixHex = "6a24aa21a9ed";

    private readonly AppConfig _cfg;
    private readonly Script _payoutScript;
    private long _jobCounter;

    public JobBuilder(AppConfig cfg)
    {
        _cfg = cfg;
        if (cfg.Coinbase.Address.StartsWith("REPLACE_", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "coinbase.address is still a placeholder. Set your fixed payout address in config.json before mining.");
        var addr = BitcoinAddress.Create(cfg.Coinbase.Address, cfg.Network);
        _payoutScript = addr.ScriptPubKey;
    }

    public ulong NextJobKey()
    {
        // Positive Int64 values provide stable lowercase-hex Stratum job ids.
        var next = Interlocked.Increment(ref _jobCounter);
        if (next <= 0)
            throw new InvalidOperationException("job id space exhausted");
        return (ulong)next;
    }

    /// <summary>
    /// Template identity + LE txid leaves without coinbase/merkle/job payloads.
    /// Leaves are reused by <see cref="FromGbt"/> so successful publish does not re-decode txids.
    /// </summary>
    internal TemplateKeyParts ComputeTemplateKeyParts(GbtResponse gbt)
    {
        _ = DecodeAndValidatePowTarget(gbt);
        ValidateGbtFields(gbt);
        var witnessHex = ResolveWitnessHex(gbt);
        var txHashes = DecodeTxidLeaves(gbt);
        var txSetFp = TxSetFingerprint(txHashes.Bytes);
        var key = BuildTemplateKey(gbt, txHashes.Count, txSetFp, witnessHex);
        return new TemplateKeyParts(key, txHashes);
    }

    /// <summary>Key-only helper (tests / call sites that discard leaves).</summary>
    public JobTemplate FromGbt(GbtResponse gbt, TemplateSource source) =>
        FromGbtCore(gbt, source, null, null);

    internal JobTemplate FromGbt(
        GbtResponse gbt,
        TemplateSource source,
        string precomputedTemplateKey,
        TxIdSet precomputedTxHashesLe) =>
        FromGbtCore(gbt, source, precomputedTemplateKey, precomputedTxHashesLe);

    private JobTemplate FromGbtCore(
        GbtResponse gbt,
        TemplateSource source,
        string? precomputedTemplateKey,
        TxIdSet? precomputedTxHashesLe)
    {
        var (nbits, targetLe) = DecodeAndValidatePowTarget(gbt);
        ValidateGbtFields(gbt);

        var prevBe = new byte[32];
        if (!BitcoinEncoding.TryDecodeExactHex(gbt.PreviousBlockhash.AsSpan(), prevBe))
            throw new InvalidOperationException("GBT previousblockhash must be exactly 32 bytes of hex");
        var prevLe = Hex.ReverseCopy(prevBe);
        var notifyPrev = SwapWords(prevBe);

        var witnessHex = ResolveWitnessHex(gbt);

        // Witness txs in the template require a coinbase commitment output (BIP141).
        if (string.IsNullOrEmpty(witnessHex))
        {
            if (gbt.PackedTransactions?.HasWitnessTransactions == true)
            {
                throw new InvalidOperationException(
                    $"GBT height={gbt.Height} contains witness transactions but no default_witness_commitment " +
                    "(enable coinbase.segwit_commitment and request rules:[\"segwit\"]).");
            }

            foreach (var tx in gbt.Transactions ?? Array.Empty<GbtTx>())
            {
                var txid = tx.TxId;
                var wtxid = tx.Hash;
                if (txid != null && wtxid != null &&
                    !txid.Equals(wtxid, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"GBT height={gbt.Height} contains witness transactions but no default_witness_commitment " +
                        "(enable coinbase.segwit_commitment and request rules:[\"segwit\"]).");
                }
            }
        }

        var (cb1, cb2, coinbaseFull) = BuildCoinbaseParts(
            (uint)gbt.Height,
            gbt.CoinbaseValue,
            gbt.CoinbaseAux?.Flags,
            witnessHex);

        var coinbaseHash = BitcoinEncoding.DoubleSha256(coinbaseFull);
        // Reuse LE txid leaves from early-skip fingerprint when provided (B3).
        var txHashes = precomputedTxHashesLe ?? DecodeTxidLeaves(gbt);
        if (precomputedTxHashesLe != null && precomputedTxHashesLe.Count != gbt.TransactionCount)
            throw new InvalidOperationException(
                $"precomputed txid leaves ({precomputedTxHashesLe.Count}) != GBT txs ({gbt.TransactionCount})");

        // Decode raw tx payloads once to binary (B2) — no long-lived hex strings in the job.
        // The RPC converter decoded each payload directly into its final binary buffer.
        var txRaw = PackTransactions(gbt);

        var mintime = gbt.Mintime ?? gbt.CurTime;
        string key;
        if (!string.IsNullOrEmpty(precomputedTemplateKey))
        {
            key = precomputedTemplateKey;
        }
        else
        {
            var txSetFp = TxSetFingerprint(txHashes.Bytes);
            key = BuildTemplateKey(gbt, txHashes.Count, txSetFp, witnessHex);
        }

        var branches = BitcoinEncoding.BuildMerkleBranches(
            coinbaseHash, txHashes.MutableBytes, txHashes.Count);

        var jobKey = NextJobKey();
        return new JobTemplate
        {
            Ready = true,
            JobId = jobKey.ToString("x"),
            JobKey = jobKey,
            SubmitOld = gbt.SubmitOld ?? true,
            TemplateKey = key,
            Source = source,
            Height = gbt.Height,
            Version = gbt.Version,
            Vbrequired = gbt.Vbrequired,
            Nbits = nbits,
            Ntime = gbt.CurTime,
            Mintime = mintime,
            CoinbaseValue = gbt.CoinbaseValue,
            NetworkDifficulty = BitsToDifficulty(nbits),
            PrevhashBe = gbt.PreviousBlockhash.ToLowerInvariant(),
            PrevhashNotifyHex = notifyPrev,
            PrevhashLe = prevLe,
            TargetLe = targetLe,
            VersionHex = Hex.U32BeHex(gbt.Version),
            NbitsHex = gbt.Bits.ToLowerInvariant().PadLeft(8, '0'),
            NtimeHex = Hex.U32BeHex(gbt.CurTime),
            Coinbase1 = cb1,
            Coinbase2 = cb2,
            Coinbase1Hex = Hex.Encode(cb1),
            Coinbase2Hex = Hex.Encode(cb2),
            MerkleBranchesLe = branches,
            MerkleBranchesHex = branches.Select(b => Hex.Encode(b)).ToList(),
            Transactions = txRaw,
            HasWitnessCommitment = !string.IsNullOrEmpty(witnessHex),
            WitnessCommitmentScriptHex = witnessHex,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    private (uint Nbits, byte[] TargetLe) DecodeAndValidatePowTarget(GbtResponse gbt)
    {
        if (gbt.Bits == null || gbt.Bits.Length != 8 ||
            !uint.TryParse(
                gbt.Bits.AsSpan(),
                NumberStyles.AllowHexSpecifier,
                CultureInfo.InvariantCulture,
                out var nbits))
        {
            throw new InvalidOperationException("GBT bits must be exactly 4 bytes of hex");
        }

        var targetLe = BitcoinEncoding.TargetHexToLe(gbt.Target);
        Span<byte> compactTargetLe = stackalloc byte[32];
        if (!BitcoinEncoding.TryCompactTargetToLe(nbits, compactTargetLe))
            throw new InvalidOperationException("GBT bits encode an invalid compact target");
        Span<byte> powLimitLe = stackalloc byte[32];
        var powLimitBits = _cfg.Network == Network.RegTest ? 0x207fffffu : 0x1d00ffffu;
        BitcoinEncoding.TryCompactTargetToLe(powLimitBits, powLimitLe);
        if (!BitcoinEncoding.LeqLe256(compactTargetLe, powLimitLe))
            throw new InvalidOperationException("GBT bits exceed the configured network proof-of-work limit");
        if (!targetLe.AsSpan().SequenceEqual(compactTargetLe))
            throw new InvalidOperationException("GBT target does not exactly match compact bits");
        return (nbits, targetLe);
    }

    private static void ValidateGbtFields(GbtResponse gbt)
    {
        if ((gbt.Version & gbt.Vbrequired) != gbt.Vbrequired)
            throw new InvalidOperationException("GBT version is missing vbrequired bits");

        Span<byte> previousBlockHash = stackalloc byte[32];
        if (gbt.PreviousBlockhash == null ||
            !BitcoinEncoding.TryDecodeExactHex(gbt.PreviousBlockhash.AsSpan(), previousBlockHash))
        {
            throw new InvalidOperationException(
                "GBT previousblockhash must be exactly 32 bytes of hex");
        }

        if (gbt.CoinbaseValue is < 0 or > MaxMoneySatoshis)
            throw new InvalidOperationException("GBT coinbasevalue is outside Bitcoin's money range");
        if (gbt.Mintime.HasValue && gbt.CurTime < gbt.Mintime.Value)
            throw new InvalidOperationException("GBT curtime must not be earlier than mintime");

        var auxFlagsHex = gbt.CoinbaseAux?.Flags;
        if (!string.IsNullOrEmpty(auxFlagsHex) && !BitcoinEncoding.IsExactHex(auxFlagsHex))
            throw new InvalidOperationException("GBT coinbaseaux.flags must be even-length hex");

        var witnessHex = gbt.DefaultWitnessCommitment;
        if (!string.IsNullOrEmpty(witnessHex) &&
            (witnessHex.Length < 76 ||
             !BitcoinEncoding.IsExactHex(witnessHex) ||
             !witnessHex.StartsWith(WitnessCommitmentPrefixHex, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException(
                "GBT default_witness_commitment must be a BIP141 commitment script");
        }
    }

    private string? ResolveWitnessHex(GbtResponse gbt)
    {
        var witnessHex = gbt.DefaultWitnessCommitment;
        if (!_cfg.Coinbase.SegwitCommitment)
            witnessHex = null;
        return witnessHex;
    }

    private static string BuildTemplateKey(
        GbtResponse gbt,
        int transactionCount,
        string transactionFingerprint,
        string? witnessHex)
    {
        var mintime = gbt.Mintime ?? gbt.CurTime;
        var submitOld = gbt.SubmitOld ?? true;
        return string.Create(
            CultureInfo.InvariantCulture,
            $"gbt:v2:{NormalizeKeyText(gbt.PreviousBlockhash)}:{gbt.Height}:{gbt.Version:x8}:" +
            $"{gbt.Vbrequired:x8}:{NormalizeKeyText(gbt.Bits)}:{NormalizeKeyText(gbt.Target)}:" +
            $"{gbt.CoinbaseValue}:{gbt.CurTime}:{mintime}:{(submitOld ? 1 : 0)}:" +
            $"{NormalizeKeyText(gbt.CoinbaseAux?.Flags)}:{transactionCount}:" +
            $"{transactionFingerprint}:{NormalizeKeyText(witnessHex)}");
    }

    private static string NormalizeKeyText(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "-" : value.Trim().ToLowerInvariant();

    private static TxIdSet DecodeTxidLeaves(GbtResponse gbt)
    {
        if (gbt.PackedTransactions != null)
            return gbt.PackedTransactions.CopyTxids();

        var txs = gbt.Transactions ?? Array.Empty<GbtTx>();
        var txHashes = new TxIdSet(txs.Length);
        for (var i = 0; i < txs.Length; i++)
        {
            // Header merkle tree uses txid (non-witness), never wtxid.
            var txid = txs[i].TxId ??
                throw new InvalidOperationException("GBT tx missing txid");
            var hash = txHashes.MutableBytes.Slice(i * 32, 32);
            if (txid.Length != 64 ||
                !BitcoinEncoding.TryDecodeExactHex(txid.AsSpan(), hash))
            {
                throw new InvalidOperationException("GBT txid must be exactly 32 bytes of hex");
            }
            hash.Reverse();
        }
        return txHashes;
    }

    private static TransactionSet PackTransactions(GbtResponse gbt)
    {
        if (gbt.PackedTransactions != null)
            return gbt.PackedTransactions.Transactions;

        var txs = gbt.Transactions ?? Array.Empty<GbtTx>();
        if (txs.Length == 0)
            return TransactionSet.Empty;

        var offsets = new int[txs.Length + 1];
        var totalBytes = 0;
        for (var i = 0; i < txs.Length; i++)
        {
            var data = txs[i].Data;
            if (data.Length == 0)
                throw new InvalidOperationException("GBT tx missing data");
            offsets[i] = totalBytes;
            totalBytes = checked(totalBytes + data.Length);
        }
        offsets[^1] = totalBytes;

        var payload = GC.AllocateUninitializedArray<byte>(totalBytes);
        for (var i = 0; i < txs.Length; i++)
            txs[i].Data.CopyTo(payload, offsets[i]);
        return new TransactionSet(payload, offsets);
    }

    public JobTemplate BuildEmptyFast(
        ChainTip tip,
        string blockHashHex,
        uint nextHeight,
        uint estimatedMtp,
        TemplateSource source = TemplateSource.P2pFast)
    {
        var subsidy = BitcoinEncoding.BlockSubsidySat(nextHeight);
        var (cb1, cb2, _) = BuildCoinbaseParts(nextHeight, subsidy, null, null);
        var blockHashBe = Hex.Decode(blockHashHex);
        var prevLe = Hex.ReverseCopy(blockHashBe);
        var notifyPrev = SwapWords(blockHashBe);
        var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var mintime = estimatedMtp + 1;
        var ntime = Math.Max(now, mintime);
        var key = $"fast:{blockHashHex.ToLowerInvariant()}:{nextHeight}:{tip.Nbits:x8}:{tip.Version:x8}";

        var jobKey = NextJobKey();
        return new JobTemplate
        {
            Ready = true,
            JobId = jobKey.ToString("x"),
            JobKey = jobKey,
            SubmitOld = false,
            TemplateKey = key,
            Source = source,
            Height = nextHeight,
            Version = tip.Version,
            Vbrequired = tip.Vbrequired,
            Nbits = tip.Nbits,
            Ntime = ntime,
            Mintime = mintime,
            CoinbaseValue = subsidy,
            NetworkDifficulty = tip.NetworkDifficulty,
            PrevhashBe = blockHashHex.ToLowerInvariant(),
            PrevhashNotifyHex = notifyPrev,
            PrevhashLe = prevLe,
            TargetLe = tip.TargetLe.ToArray(),
            VersionHex = Hex.U32BeHex(tip.Version),
            NbitsHex = Hex.U32BeHex(tip.Nbits),
            NtimeHex = Hex.U32BeHex(ntime),
            Coinbase1 = cb1,
            Coinbase2 = cb2,
            Coinbase1Hex = Hex.Encode(cb1),
            Coinbase2Hex = Hex.Encode(cb2),
            MerkleBranchesLe = new List<byte[]>(),
            MerkleBranchesHex = new List<string>(),
            Transactions = TransactionSet.Empty,
            HasWitnessCommitment = false,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    private (byte[] cb1, byte[] cb2, byte[] fullLegacy) BuildCoinbaseParts(
        uint height,
        long valueSat,
        string? auxFlagsHex,
        string? witnessCommitmentHex)
    {
        // coinbase1 = version|txin_count|prevout_hash|prevout_index|scriptsig_len|scriptsig_prefix(height+tag)
        // extranonce1+2 inserted by miner between cb1 and cb2
        // coinbase2 = sequence|outs|locktime  (and witness commitment out if present)

        // scriptSig without extranonces
        using var script = new MemoryStream();
        using var sw = new BinaryWriter(script);
        var heightPush = BitcoinEncoding.EncodeCoinbaseHeight(height);
        sw.Write(heightPush);
        // scriptSig tag = coinbase.message from config only (no hardcoded product prefix).
        var message = _cfg.Coinbase.Message ?? "";
        if (message.Length > 0)
        {
            var tag = System.Text.Encoding.UTF8.GetBytes(message);
            if (tag.Length > 40) tag = tag[..40];
            BitcoinEncoding.WritePushData(sw, tag);
        }
        if (!string.IsNullOrEmpty(auxFlagsHex))
        {
            if ((auxFlagsHex.Length & 1) != 0)
                throw new InvalidOperationException("GBT coinbaseaux.flags must be even-length hex");
            var flags = new byte[auxFlagsHex.Length / 2];
            if (!BitcoinEncoding.TryDecodeExactHex(auxFlagsHex.AsSpan(), flags))
                throw new InvalidOperationException("GBT coinbaseaux.flags must be even-length hex");
            sw.Write(flags);
        }

        var scriptPrefix = script.ToArray();
        var extranonceLen = _cfg.Stratum.Extranonce1Size + _cfg.Stratum.Extranonce2Size;
        var scriptSigLen = scriptPrefix.Length + extranonceLen;
        // Consensus: coinbase scriptSig must be 2..100 bytes (BIP34 height push already ensures >=2).
        if (scriptSigLen is < 2 or > 100)
            throw new InvalidOperationException(
                $"coinbase scriptSig length {scriptSigLen} out of range 2..100 (shorten coinbase.message or extranonce sizes)");

        // Build coinbase1 up to and including script prefix (length includes EN space)
        using var cb1ms = new MemoryStream();
        using var cb1w = new BinaryWriter(cb1ms);
        cb1w.Write(1u);
        BitcoinEncoding.WriteVarInt(cb1w, 1);
        cb1w.Write(new byte[32]);
        cb1w.Write(0xffffffff);
        BitcoinEncoding.WriteVarInt(cb1w, (ulong)scriptSigLen);
        cb1w.Write(scriptPrefix);
        var coinbase1 = cb1ms.ToArray();

        using var cb2ms = new MemoryStream();
        using var cb2w = new BinaryWriter(cb2ms);
        cb2w.Write(0xffffffff); // sequence

        var hasWitness = !string.IsNullOrEmpty(witnessCommitmentHex);
        BitcoinEncoding.WriteVarInt(cb2w, hasWitness ? 2u : 1u);

        // payout output — FIXED ADDRESS
        cb2w.Write((ulong)valueSat);
        var scriptBytes = _payoutScript.ToBytes(true);
        BitcoinEncoding.WriteVarInt(cb2w, (ulong)scriptBytes.Length);
        cb2w.Write(scriptBytes);

        if (hasWitness)
        {
            var commitment = Hex.Decode(witnessCommitmentHex!);
            cb2w.Write(0UL);
            BitcoinEncoding.WriteVarInt(cb2w, (ulong)commitment.Length);
            cb2w.Write(commitment);
        }

        cb2w.Write(0u); // locktime
        var coinbase2 = cb2ms.ToArray();

        // full legacy with zero extranonce for merkle scaffolding of empty jobs
        using var fullMs = new MemoryStream();
        fullMs.Write(coinbase1);
        fullMs.Write(new byte[extranonceLen]);
        fullMs.Write(coinbase2);

        return (coinbase1, coinbase2, fullMs.ToArray());
    }

    /// <summary>
    /// Stratum mining.notify prevhash encoding (Miningcore ReverseByteOrder):
    /// start from RPC big-endian previousblockhash, reverse the order of its eight 4-byte words
    /// (bytes inside each word stay in RPC order). Equivalent to: full LE reverse, then reverse each uint32.
    /// </summary>
    private static string SwapWords(byte[] be32)
    {
        if (be32.Length != 32)
            throw new ArgumentException("prevhash must be 32 bytes");
        var outBytes = new byte[32];
        for (var i = 0; i < 8; i++)
            Buffer.BlockCopy(be32, i * 4, outBytes, (7 - i) * 4, 4);
        return Hex.Encode(outBytes);
    }

    private static double BitsToDifficulty(uint nbits)
    {
        var mantissa = nbits & 0x007fffff;
        var exponent = (int)((nbits >> 24) & 0xff);
        var target = mantissa * Math.Pow(256, exponent - 3);
        var diff1 = 0xffffUL * Math.Pow(2, 208);
        return target > 0 ? diff1 / target : 0;
    }

    /// <summary>
    /// Stable 128-bit fingerprint of the GBT transaction set (order-sensitive).
    /// Uses LE txids already decoded for merkle construction — O(n) SHA256, not full tx hex.
    /// </summary>
    internal static string TxSetFingerprint(ReadOnlySpan<byte> txHashesLe)
    {
        if ((txHashesLe.Length & 31) != 0)
            throw new InvalidOperationException("txid buffer length must be a multiple of 32");

        Span<byte> digest = stackalloc byte[32];
        if (!SHA256.TryHashData(txHashesLe, digest, out var written) || written != digest.Length)
            throw new CryptographicException("SHA256.TryHashData failed for transaction fingerprint");
        return Hex.Encode(digest[..16]);
    }
}

/// <summary>Result of early template fingerprint: key string + LE txid leaves for FromGbt reuse.</summary>
internal readonly record struct TemplateKeyParts(string Key, TxIdSet TxHashesLe);
