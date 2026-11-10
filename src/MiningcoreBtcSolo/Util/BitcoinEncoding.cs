using System.Buffers.Binary;
using System.Security.Cryptography;

namespace MiningcoreBtcSolo.Util;

public static class BitcoinEncoding
{
    public const double MaxSupportedShareDifficulty = 1e18;

    public static void WriteVarInt(BinaryWriter w, ulong value)
    {
        if (value < 0xfd)
            w.Write((byte)value);
        else if (value <= 0xffff)
        {
            w.Write((byte)0xfd);
            w.Write((ushort)value);
        }
        else if (value <= 0xffffffff)
        {
            w.Write((byte)0xfe);
            w.Write((uint)value);
        }
        else
        {
            w.Write((byte)0xff);
            w.Write(value);
        }
    }

    public static int VarIntLength(ulong value) =>
        value < 0xfd ? 1 : value <= 0xffff ? 3 : value <= 0xffffffff ? 5 : 9;

    public static int WriteVarInt(Span<byte> destination, ulong value)
    {
        var length = VarIntLength(value);
        if (destination.Length < length)
            throw new ArgumentException("destination is too short for varint", nameof(destination));

        switch (length)
        {
            case 1:
                destination[0] = (byte)value;
                break;
            case 3:
                destination[0] = 0xfd;
                BinaryPrimitives.WriteUInt16LittleEndian(destination[1..], (ushort)value);
                break;
            case 5:
                destination[0] = 0xfe;
                BinaryPrimitives.WriteUInt32LittleEndian(destination[1..], (uint)value);
                break;
            default:
                destination[0] = 0xff;
                BinaryPrimitives.WriteUInt64LittleEndian(destination[1..], value);
                break;
        }
        return length;
    }

    /// <summary>
    /// Double-SHA256 into a 32-byte destination (no heap). Preferred on share hot path.
    /// </summary>
    public static void DoubleSha256(ReadOnlySpan<byte> data, Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("destination must be at least 32 bytes", nameof(destination));

        Span<byte> first = stackalloc byte[32];
        if (!SHA256.TryHashData(data, first, out var w1) || w1 != 32)
            throw new CryptographicException("SHA256.TryHashData failed (first pass)");
        if (!SHA256.TryHashData(first, destination[..32], out var w2) || w2 != 32)
            throw new CryptographicException("SHA256.TryHashData failed (second pass)");
    }

    /// <summary>Double-SHA256 returning a new 32-byte array (job build / rare paths).</summary>
    public static byte[] DoubleSha256(ReadOnlySpan<byte> data)
    {
        var result = new byte[32];
        DoubleSha256(data, result);
        return result;
    }

    /// <summary>Merkle parent hash into a 32-byte destination (concat on stack, no heap).</summary>
    public static void MerkleStep(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> destination)
    {
        if (left.Length != 32 || right.Length != 32)
            throw new ArgumentException("merkle leaves must be 32 bytes");
        if (destination.Length < 32)
            throw new ArgumentException("destination must be at least 32 bytes", nameof(destination));

        Span<byte> buf = stackalloc byte[64];
        left.CopyTo(buf);
        right.CopyTo(buf[32..]);
        DoubleSha256(buf, destination);
    }

    public static byte[] MerkleStep(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var result = new byte[32];
        MerkleStep(left, right, result);
        return result;
    }

    /// <summary>
    /// Builds the coinbase Merkle branch while reducing a contiguous txid buffer in place.
    /// Only the O(log n) returned branch hashes allocate individual arrays.
    /// </summary>
    internal static List<byte[]> BuildMerkleBranches(
        ReadOnlySpan<byte> coinbaseHashLe,
        Span<byte> txHashesLe,
        int transactionCount)
    {
        var branches = new List<byte[]>();
        if (coinbaseHashLe.Length != 32)
            throw new ArgumentException("coinbase hash must be 32 bytes", nameof(coinbaseHashLe));
        if (transactionCount < 0 || txHashesLe.Length != checked(transactionCount * 32))
            throw new ArgumentException("txid buffer length does not match transaction count", nameof(txHashesLe));
        if (transactionCount == 0)
            return branches;

        // Level zero is virtual: [coinbase, tx0, tx1, ...]. Parent hashes are written
        // over txids that have already been consumed, so no second level buffer is needed.
        branches.Add(txHashesLe[..32].ToArray());
        var currentCount = transactionCount + 1;
        var nextCount = (currentCount + 1) / 2;
        for (var output = 0; output < nextCount; output++)
        {
            var leftIndex = output * 2;
            var rightIndex = Math.Min(leftIndex + 1, currentCount - 1);
            var left = leftIndex == 0
                ? coinbaseHashLe
                : txHashesLe.Slice((leftIndex - 1) * 32, 32);
            var right = rightIndex == 0
                ? coinbaseHashLe
                : txHashesLe.Slice((rightIndex - 1) * 32, 32);
            MerkleStep(left, right, txHashesLe.Slice(output * 32, 32));
        }

        currentCount = nextCount;
        while (currentCount > 1)
        {
            branches.Add(txHashesLe.Slice(32, 32).ToArray());
            nextCount = (currentCount + 1) / 2;
            for (var output = 0; output < nextCount; output++)
            {
                var leftIndex = output * 2;
                var rightIndex = Math.Min(leftIndex + 1, currentCount - 1);
                var left = txHashesLe.Slice(leftIndex * 32, 32);
                var right = txHashesLe.Slice(rightIndex * 32, 32);
                MerkleStep(left, right, txHashesLe.Slice(output * 32, 32));
            }
            currentCount = nextCount;
        }

        return branches;
    }

    /// <summary>Write an 80-byte block header into <paramref name="header"/> (no heap).</summary>
    public static void BuildHeader(
        uint version,
        ReadOnlySpan<byte> prevhashLe,
        ReadOnlySpan<byte> merkleRootLe,
        uint ntime,
        uint nbits,
        uint nonce,
        Span<byte> header)
    {
        if (header.Length < 80)
            throw new ArgumentException("header must be at least 80 bytes", nameof(header));
        if (prevhashLe.Length < 32 || merkleRootLe.Length < 32)
            throw new ArgumentException("prevhash and merkle root must be 32 bytes");

        BinaryPrimitives.WriteUInt32LittleEndian(header[..4], version);
        prevhashLe[..32].CopyTo(header.Slice(4, 32));
        merkleRootLe[..32].CopyTo(header.Slice(36, 32));
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(68, 4), ntime);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(72, 4), nbits);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(76, 4), nonce);
    }

    public static byte[] BuildHeader(
        uint version,
        ReadOnlySpan<byte> prevhashLe,
        ReadOnlySpan<byte> merkleRootLe,
        uint ntime,
        uint nbits,
        uint nonce)
    {
        var header = new byte[80];
        BuildHeader(version, prevhashLe, merkleRootLe, ntime, nbits, nonce, header);
        return header;
    }

    /// <summary>Compare two 256-bit little-endian integers: a &lt;= b.</summary>
    public static bool LeqLe256(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        for (var i = 31; i >= 0; i--)
        {
            if (a[i] < b[i]) return true;
            if (a[i] > b[i]) return false;
        }
        return true;
    }

    public static byte[] CompactTargetToLe(uint nbits)
    {
        var target = new byte[32];
        TryCompactTargetToLe(nbits, target);
        return target;
    }

    /// <summary>
    /// Decode Bitcoin's compact target and reject negative, zero, or overflowing values
    /// using the same validity rules as Bitcoin Core's DeriveTarget/SetCompact path.
    /// </summary>
    public static bool TryCompactTargetToLe(uint nbits, Span<byte> target)
    {
        if (target.Length < 32)
            throw new ArgumentException("target must be at least 32 bytes", nameof(target));

        target[..32].Clear();
        var exponent = (int)(nbits >> 24);
        var mantissa = nbits & 0x007fffff;
        // Match Bitcoin Core/NBitcoin SetCompact overflow rules. Exponent 33 or 34
        // can still describe a 256-bit value when the mantissa has enough leading zeroes.
        var overflow = mantissa != 0 &&
                       (exponent > 34 ||
                         (mantissa > 0xff && exponent > 33) ||
                         (mantissa > 0xffff && exponent > 32));
        if ((nbits & 0x00800000) != 0 || mantissa == 0 || exponent == 0 || overflow)
            return false;

        if (exponent <= 3)
        {
            mantissa >>= 8 * (3 - (int)exponent);
            if (mantissa == 0)
                return false;
            for (var i = 0; i < exponent; i++)
                target[i] = (byte)((mantissa >> (8 * i)) & 0xff);
            return true;
        }

        // The compact target is mantissa * 256^(exponent - 3). The result is
        // stored little-endian because LeqLe256 compares hash bytes as an LE integer.
        var pos = exponent - 3;
        for (var i = 0; i < 3; i++)
        {
            var index = pos + i;
            if (index >= 0 && index < 32)
                target[index] = (byte)(mantissa >> (8 * i));
        }
        return true;
    }

    public static byte[] TargetHexToLe(string targetHex)
    {
        if (targetHex == null || targetHex.Length != 64)
            throw new FormatException("target must be exactly 32 bytes of hex");

        var be = new byte[32];
        if (!TryDecodeExactHex(targetHex.AsSpan(), be))
            throw new FormatException("target must be exactly 32 bytes of hex");
        Array.Reverse(be);
        return be;
    }

    internal static bool TryDecodeExactHex(ReadOnlySpan<char> hex, Span<byte> destination)
    {
        if (hex.Length != checked(destination.Length * 2) || !IsExactHex(hex))
            return false;
        if (hex.IsEmpty)
            return true;
        return Hex.TryDecode(hex, destination, out var written) && written == destination.Length;
    }

    internal static bool IsExactHex(ReadOnlySpan<char> hex)
    {
        if ((hex.Length & 1) != 0)
            return false;
        foreach (var c in hex)
        {
            if (c is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f') and
                not (>= 'A' and <= 'F'))
                return false;
        }
        return true;
    }

    public static byte[] DiffToShareTargetLe(double diff)
    {
        // DIFF1 (BE) / diff → LE target.
        // Convert the IEEE-754 value to an exact rational so high configured
        // difficulties never overflow an intermediate decimal or integer type.
        if (!double.IsFinite(diff) || diff <= 0)
            diff = 1;
        if (diff > MaxSupportedShareDifficulty)
            diff = MaxSupportedShareDifficulty;

        var (numerator, denominator) = PositiveDoubleToRational(diff);
        var target = Diff1Target * denominator / numerator;
        if (target.Sign <= 0)
            target = 1;

        var be = target.ToByteArray(isUnsigned: true, isBigEndian: true);
        if (be.Length > 32)
            return Enumerable.Repeat((byte)0xff, 32).ToArray();
        var padded = new byte[32];
        Buffer.BlockCopy(be, 0, padded, 32 - be.Length, be.Length);
        Array.Reverse(padded);
        return padded;
    }

    private static (System.Numerics.BigInteger Numerator, System.Numerics.BigInteger Denominator)
        PositiveDoubleToRational(double value)
    {
        var bits = BitConverter.DoubleToInt64Bits(value);
        var exponentBits = (int)((bits >> 52) & 0x7ff);
        var fraction = bits & 0x000f_ffff_ffff_ffffL;

        long significand;
        int exponent;
        if (exponentBits == 0)
        {
            significand = fraction;
            exponent = -1022 - 52;
        }
        else
        {
            significand = fraction | (1L << 52);
            exponent = exponentBits - 1023 - 52;
        }

        var numerator = new System.Numerics.BigInteger(significand);
        var denominator = System.Numerics.BigInteger.One;
        if (exponent >= 0)
            numerator <<= exponent;
        else
            denominator <<= -exponent;

        return (numerator, denominator);
    }

    /// <summary>
    /// DIFF1 / hash as display difficulty. Keeps BigInteger math (VarDiff grace needs accuracy);
    /// caches DIFF1 and avoids heap reverse buffer on the hot accept path (B4-safe).
    /// </summary>
    public static double HashToDisplayDiff(ReadOnlySpan<byte> hashLe)
    {
        if (hashLe.Length != 32)
        {
            // Defensive: rare; fall back without stackalloc size assumptions.
            var copy = hashLe.ToArray();
            Array.Reverse(copy);
            return HashToDisplayDiffFromBe(copy);
        }

        Span<byte> be = stackalloc byte[32];
        for (var i = 0; i < 32; i++)
            be[i] = hashLe[31 - i];
        return HashToDisplayDiffFromBe(be);
    }

    private static readonly System.Numerics.BigInteger Diff1Target =
        System.Numerics.BigInteger.Parse(
            "00000000ffff0000000000000000000000000000000000000000000000000000",
            System.Globalization.NumberStyles.HexNumber);

    private static double HashToDisplayDiffFromBe(ReadOnlySpan<byte> hashBe)
    {
        var hashVal = new System.Numerics.BigInteger(hashBe, isUnsigned: true, isBigEndian: true);
        if (hashVal.IsZero)
            return double.PositiveInfinity;
        var q = System.Numerics.BigInteger.DivRem(Diff1Target, hashVal, out var rem);
        return (double)q + (double)rem / (double)hashVal;
    }

    public static byte[] EncodeCoinbaseHeight(uint height)
    {
        // BIP34 uses the minimally encoded positive ScriptNum. If the high bit
        // would be interpreted as a sign bit, append a zero byte before pushing.
        if (height == 0)
            return new byte[] { 0x00 };
        if (height <= 16)
            return new byte[] { (byte)(0x50 + height) }; // OP_1 .. OP_16

        var valueBytes = height <= 0xff
            ? 1
            : height <= 0xffff
                ? 2
                : height <= 0xffffff
                    ? 3
                    : 4;
        var needsSignByte = ((height >> (8 * (valueBytes - 1))) & 0x80) != 0;
        var payloadBytes = valueBytes + (needsSignByte ? 1 : 0);
        var result = new byte[1 + payloadBytes];
        result[0] = (byte)payloadBytes;
        for (var i = 0; i < valueBytes; i++)
            result[1 + i] = (byte)(height >> (8 * i));
        return result;
    }

    public static void WritePushData(BinaryWriter w, ReadOnlySpan<byte> data)
    {
        if (data.Length < 0x4c)
            w.Write((byte)data.Length);
        else if (data.Length <= 0xff)
        {
            w.Write((byte)0x4c);
            w.Write((byte)data.Length);
        }
        else if (data.Length <= 0xffff)
        {
            w.Write((byte)0x4d);
            w.Write((ushort)data.Length);
        }
        else
        {
            w.Write((byte)0x4e);
            w.Write((uint)data.Length);
        }
        w.Write(data);
    }

    public static long BlockSubsidySat(uint height)
    {
        // 50 BTC >> halvings
        var halvings = height / 210_000u;
        if (halvings >= 64)
            return 0;
        return 5_000_000_000L >> (int)halvings;
    }
}
