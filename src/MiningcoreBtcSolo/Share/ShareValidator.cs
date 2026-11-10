using System.Buffers;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Share;

public sealed class ShareSubmit
{
    public string Extranonce2 { get; init; } = "";
    public string Ntime { get; init; } = "";
    public string Nonce { get; init; } = "";
    public string? Version { get; init; }
}

public readonly record struct ShareResult(
    bool Accepted,
    bool IsBlock,
    Hash256 Hash,
    double ActualDiff,
    BlockCandidate? BlockCandidate)
{
    /// <summary>True once the submitted header was hashed, including low-difficulty rejects.</summary>
    internal bool HashComputed { get; init; }

    /// <summary>Compatibility/testing view. Production submission keeps binary bytes.</summary>
    public string? BlockHex => BlockCandidate?.GetHex();
}

public static class ShareValidator
{
    /// <summary>Solo coinbase is consensus-bounded (scriptSig ≤100); 512B covers witness commitment + margin.</summary>
    private const int MaxStackCoinbase = 512;

    /// <summary>extranonce2_size config max is 8; allow a little headroom for direct callers.</summary>
    private const int MaxStackEn2 = 32;

    public static ShareResult Validate(
        JobTemplate job,
        byte[] coinbasePrefix, // coinbase1 + extranonce1
        ShareSubmit submit,
        byte[] shareTargetLe)
        => Validate(job, coinbasePrefix.AsSpan(), submit, shareTargetLe);

    public static ShareResult Validate(
        JobTemplate job,
        ReadOnlySpan<byte> coinbasePrefix,
        ShareSubmit submit,
        byte[] shareTargetLe)
    {
        if (!Hex.TryParseU32Be(submit.Ntime, out var ntime) ||
            !Hex.TryParseU32Be(submit.Nonce, out var nonce))
            return Reject();

        if (job.Mintime != 0 && ntime < job.Mintime)
            return Reject();
        var now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (ntime > now + 7200)
            return Reject();

        var en2Text = submit.Extranonce2.AsSpan().Trim();
        var maxDecodedLength = (en2Text.Length + 1) / 2;
        if (maxDecodedLength <= MaxStackEn2)
        {
            Span<byte> en2 = stackalloc byte[MaxStackEn2];
            if (!Hex.TryDecode(en2Text, en2, out var en2Len) || en2Len == 0)
                return Reject();

            return ValidateDecodedWithVersionText(
                job, coinbasePrefix, en2[..en2Len], ntime, nonce, submit.Version, shareTargetLe);
        }

        // Oversized extranonce2 is not reachable from validated Stratum config, but direct
        // callers retain the historical heap fallback instead of being rejected by the stack cap.
        if (!Hex.TryDecode(submit.Extranonce2, out var en2Heap) || en2Heap.Length == 0)
            return Reject();

        return ValidateDecodedWithVersionText(
            job, coinbasePrefix, en2Heap, ntime, nonce, submit.Version, shareTargetLe);
    }

    /// <summary>
    /// Validate an already parsed Stratum submission. The caller owns version-rolling policy;
    /// this method only performs consensus/share checks and does not retain any spans.
    /// </summary>
    public static ShareResult Validate(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> extranonce2,
        uint ntime,
        uint nonce,
        uint version,
        scoped ReadOnlySpan<byte> shareTargetLe)
    {
        if (extranonce2.IsEmpty || !IsTimestampValid(job, ntime))
            return Reject();

        return ValidateDecodedCore(
            job, coinbasePrefix, extranonce2, ntime, nonce, version, shareTargetLe);
    }

    /// <summary>
    /// Compute the merkle root for the session/job extranonce tuple into caller-owned storage.
    /// </summary>
    public static void ComputeMerkleRoot(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> extranonce2,
        scoped Span<byte> destination)
    {
        if (extranonce2.IsEmpty)
            throw new ArgumentException("extranonce2 is required", nameof(extranonce2));
        if (destination.Length < 32)
            throw new ArgumentException("destination must be at least 32 bytes", nameof(destination));

        var coinbaseLen = checked(coinbasePrefix.Length + extranonce2.Length + job.Coinbase2.Length);
        if (coinbaseLen <= MaxStackCoinbase)
        {
            Span<byte> coinbase = stackalloc byte[coinbaseLen];
            AssembleCoinbase(job, coinbasePrefix, extranonce2, coinbase);
            ComputeMerkleRoot(job, coinbase, destination);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(coinbaseLen);
        try
        {
            var coinbase = rented.AsSpan(0, coinbaseLen);
            AssembleCoinbase(job, coinbasePrefix, extranonce2, coinbase);
            ComputeMerkleRoot(job, coinbase, destination);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Validate a header using a previously computed merkle root.</summary>
    public static ShareResult ValidateWithMerkleRoot(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> extranonce2,
        scoped ReadOnlySpan<byte> merkleRootLe,
        uint ntime,
        uint nonce,
        uint version,
        scoped ReadOnlySpan<byte> shareTargetLe)
    {
        if (extranonce2.IsEmpty || !IsTimestampValid(job, ntime))
            return Reject();
        if (merkleRootLe.Length != 32)
            throw new ArgumentException("merkle root must be 32 bytes", nameof(merkleRootLe));

        return ValidateHeader(
            job, coinbasePrefix, extranonce2, merkleRootLe,
            ntime, nonce, version, shareTargetLe, default);
    }

    private static ShareResult ValidateDecodedWithVersionText(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2,
        uint ntime,
        uint nonce,
        string? submittedVersion,
        scoped ReadOnlySpan<byte> shareTargetLe)
    {
        var version = job.Version;
        if (submittedVersion != null)
        {
            if (!Hex.TryParseU32Be(submittedVersion, out var v))
                return Reject();
            version = v;
        }

        return ValidateDecodedCore(job, coinbasePrefix, en2, ntime, nonce, version, shareTargetLe);
    }

    private static ShareResult ValidateDecodedCore(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2,
        uint ntime,
        uint nonce,
        uint version,
        scoped ReadOnlySpan<byte> shareTargetLe)
    {

        var coinbaseLen = checked(coinbasePrefix.Length + en2.Length + job.Coinbase2.Length);
        if (coinbaseLen <= MaxStackCoinbase)
        {
            Span<byte> coinbase = stackalloc byte[coinbaseLen];
            return ValidateWithCoinbase(
                job, coinbasePrefix, en2, ntime, nonce, version, shareTargetLe, coinbase);
        }

        var coinbaseRented = ArrayPool<byte>.Shared.Rent(coinbaseLen);
        try
        {
            return ValidateWithCoinbase(
                job, coinbasePrefix, en2, ntime, nonce, version, shareTargetLe,
                coinbaseRented.AsSpan(0, coinbaseLen));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(coinbaseRented);
        }
    }

    private static ShareResult ValidateWithCoinbase(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2,
        uint ntime,
        uint nonce,
        uint version,
        scoped ReadOnlySpan<byte> shareTargetLe,
        scoped Span<byte> coinbase)
    {
        AssembleCoinbase(job, coinbasePrefix, en2, coinbase);

        // Dual 32-byte slots for merkle walk — no intermediate heap hashes.
        Span<byte> merkle = stackalloc byte[32];
        ComputeMerkleRoot(job, coinbase, merkle);
        return ValidateHeader(
            job, coinbasePrefix, en2, merkle,
            ntime, nonce, version, shareTargetLe, coinbase);
    }

    private static ShareResult ValidateHeader(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2,
        scoped ReadOnlySpan<byte> merkle,
        uint ntime,
        uint nonce,
        uint version,
        scoped ReadOnlySpan<byte> shareTargetLe,
        scoped ReadOnlySpan<byte> assembledCoinbase)
    {

        Span<byte> header = stackalloc byte[80];
        BitcoinEncoding.BuildHeader(version, job.PrevhashLe, merkle, ntime, job.Nbits, nonce, header);

        Span<byte> hashLe = stackalloc byte[32];
        BitcoinEncoding.DoubleSha256(header, hashLe);

        var accepted = BitcoinEncoding.LeqLe256(hashLe, shareTargetLe);
        var isBlock = BitcoinEncoding.LeqLe256(hashLe, job.TargetLe);
        var hash = Hash256.FromLittleEndian(hashLe);

        // Low-diff rejects retain the allocation-free hash for diagnostics, but still skip
        // BigInteger display-difficulty and hex encoding on the flood-sensitive hot path.
        if (!accepted && !isBlock)
        {
            return new ShareResult(false, false, hash, 0, null)
            {
                HashComputed = true
            };
        }

        var diff = BitcoinEncoding.HashToDisplayDiff(hashLe);
        BlockCandidate? blockCandidate = null;
        if (isBlock)
        {
            // Rare path: retain one binary block buffer through submitblock. Avoid a
            // binary -> UTF-16 hex -> UTF-8 conversion chain on the latency-critical path.
            var coinbaseArr = assembledCoinbase.IsEmpty
                ? AssembleCoinbase(job, coinbasePrefix, en2)
                : assembledCoinbase.ToArray();
            blockCandidate = BuildBlockCandidate(
                header, coinbaseArr, job.Transactions, job.HasWitnessCommitment);
        }

        return new ShareResult(accepted, isBlock, hash, diff, blockCandidate)
        {
            HashComputed = true
        };
    }

    private static void AssembleCoinbase(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2,
        scoped Span<byte> destination)
    {
        coinbasePrefix.CopyTo(destination);
        en2.CopyTo(destination[coinbasePrefix.Length..]);
        job.Coinbase2.CopyTo(destination[(coinbasePrefix.Length + en2.Length)..]);
    }

    private static byte[] AssembleCoinbase(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbasePrefix,
        scoped ReadOnlySpan<byte> en2)
    {
        var result = new byte[checked(coinbasePrefix.Length + en2.Length + job.Coinbase2.Length)];
        AssembleCoinbase(job, coinbasePrefix, en2, result);
        return result;
    }

    private static void ComputeMerkleRoot(
        JobTemplate job,
        scoped ReadOnlySpan<byte> coinbase,
        scoped Span<byte> destination)
    {
        Span<byte> a = stackalloc byte[32];
        Span<byte> b = stackalloc byte[32];
        BitcoinEncoding.DoubleSha256(coinbase, a);
        var currentInA = true;
        foreach (var branch in job.MerkleBranchesLe)
        {
            if (currentInA)
                BitcoinEncoding.MerkleStep(a, branch, b);
            else
                BitcoinEncoding.MerkleStep(b, branch, a);
            currentInA = !currentInA;
        }
        (currentInA ? a : b).CopyTo(destination);
    }

    private static bool IsTimestampValid(JobTemplate job, uint ntime)
    {
        if (job.Mintime != 0 && ntime < job.Mintime)
            return false;
        var now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        return ntime <= now + 7200;
    }

    private static ShareResult Reject() => default;

    private static BlockCandidate BuildBlockCandidate(
        ReadOnlySpan<byte> header,
        ReadOnlySpan<byte> coinbaseLegacy,
        TransactionSet txs,
        bool witness)
    {
        var txCount = (ulong)txs.Count + 1;
        var binaryLength = checked(
            header.Length +
            BitcoinEncoding.VarIntLength(txCount) +
            coinbaseLegacy.Length +
            (witness ? 36 : 0) +
            txs.SerializedLength);

        var block = GC.AllocateUninitializedArray<byte>(binaryLength);
        var destination = block.AsSpan();
        var offset = 0;
        header.CopyTo(destination[offset..]);
        offset += header.Length;

        offset += BitcoinEncoding.WriteVarInt(destination[offset..], txCount);
        if (witness)
            WriteWitnessCoinbase(coinbaseLegacy, destination, ref offset);
        else
        {
            coinbaseLegacy.CopyTo(destination[offset..]);
            offset += coinbaseLegacy.Length;
        }

        txs.SerializedBytes.CopyTo(destination[offset..]);
        offset += txs.SerializedLength;
        if (offset != destination.Length)
            throw new InvalidOperationException("block length calculation mismatch");
        return new BlockCandidate(block);
    }

    /// <summary>
    /// BIP141: coinbase witness serialization for submitblock.
    /// legacy = version|vin|vout|locktime (no marker/flag/witness).
    /// witness = version|0x00|0x01|vin|vout|stack(1 item of 32 zero bytes)|locktime.
    /// Reserved value must be 32 zero bytes to match GBT default_witness_commitment.
    /// </summary>
    private static void WriteWitnessCoinbase(
        ReadOnlySpan<byte> legacy,
        Span<byte> destination,
        ref int offset)
    {
        if (legacy.Length < 8)
            throw new InvalidOperationException("coinbase too short");

        legacy[..4].CopyTo(destination[offset..]);
        offset += 4;
        destination[offset++] = 0x00;
        destination[offset++] = 0x01;
        legacy.Slice(4, legacy.Length - 8).CopyTo(destination[offset..]);
        offset += legacy.Length - 8;
        destination[offset++] = 0x01;
        destination[offset++] = 0x20;
        destination.Slice(offset, 32).Clear();
        offset += 32;
        legacy[^4..].CopyTo(destination[offset..]);
        offset += 4;
    }
}
