using System.Globalization;

namespace MiningcoreBtcSolo.Util;

/// <summary>
/// Fast hex helpers (lookup tables). Output is always lowercase — matches historical Encode("x2").
/// </summary>
public static class Hex
{
    private static readonly char[] NibbleLower =
    [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    ];

    // 0..127 ASCII → nibble, 0xFF = invalid
    private static readonly byte[] NibbleValue = CreateNibbleValue();

    private static byte[] CreateNibbleValue()
    {
        var t = new byte[128];
        Array.Fill(t, (byte)0xFF);
        for (var c = '0'; c <= '9'; c++)
            t[c] = (byte)(c - '0');
        for (var c = 'a'; c <= 'f'; c++)
            t[c] = (byte)(c - 'a' + 10);
        for (var c = 'A'; c <= 'F'; c++)
            t[c] = (byte)(c - 'A' + 10);
        return t;
    }

    public static byte[] Decode(string hex)
    {
        // Empty / whitespace → empty buffer (historical behavior).
        if (string.IsNullOrWhiteSpace(hex))
            return Array.Empty<byte>();
        if (!TryDecode(hex, out var bytes))
            throw new FormatException("Invalid hex string");
        return bytes;
    }

    public static bool TryDecode(string? hex, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(hex))
            return false;

        var s = hex.AsSpan().Trim();
        if (s.Length >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
            s = s[2..];
        if (s.Length == 0)
            return false;

        // Odd length: left-pad one nibble (same as previous byte.Parse path).
        var byteLen = (s.Length + 1) / 2;
        var result = new byte[byteLen];
        if (!TryDecodeCore(s, result, out var written) || written != byteLen)
            return false;

        bytes = result;
        return true;
    }

    /// <summary>
    /// Decode hex into <paramref name="destination"/> without heap allocation.
    /// Accepts optional 0x prefix and odd-length left-pad (same rules as <see cref="TryDecode"/>).
    /// </summary>
    public static bool TryDecode(ReadOnlySpan<char> hex, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        var s = hex.Trim();
        if (s.Length >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
            s = s[2..];
        if (s.Length == 0)
            return false;

        var byteLen = (s.Length + 1) / 2;
        if (destination.Length < byteLen)
            return false;
        return TryDecodeCore(s, destination, out bytesWritten);
    }

    private static bool TryDecodeCore(ReadOnlySpan<char> s, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        var odd = (s.Length & 1) != 0;
        var byteLen = (s.Length + 1) / 2;
        if (destination.Length < byteLen)
            return false;

        var si = 0;
        var di = 0;
        if (odd)
        {
            if (!TryNibble(s[0], out var lo))
                return false;
            destination[0] = lo;
            si = 1;
            di = 1;
        }

        for (; si < s.Length; si += 2, di++)
        {
            if (!TryNibble(s[si], out var hi) || !TryNibble(s[si + 1], out var lo))
                return false;
            destination[di] = (byte)((hi << 4) | lo);
        }

        bytesWritten = byteLen;
        return true;
    }

    private static bool TryNibble(char c, out byte value)
    {
        value = 0;
        if (c > 127)
            return false;
        var v = NibbleValue[c];
        if (v == 0xFF)
            return false;
        value = v;
        return true;
    }

    public static string Encode(byte[] data)
    {
        if (data.Length == 0)
            return "";
        return string.Create(data.Length * 2, data, static (chars, bytes) =>
        {
            for (var i = 0; i < bytes.Length; i++)
            {
                var b = bytes[i];
                chars[i * 2] = NibbleLower[b >> 4];
                chars[i * 2 + 1] = NibbleLower[b & 0xF];
            }
        });
    }

    public static string Encode(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return "";
        var chars = new char[data.Length * 2];
        for (var i = 0; i < data.Length; i++)
        {
            var b = data[i];
            chars[i * 2] = NibbleLower[b >> 4];
            chars[i * 2 + 1] = NibbleLower[b & 0xF];
        }
        return new string(chars);
    }

    public static void Encode(ReadOnlySpan<byte> data, Span<char> destination)
    {
        if (destination.Length < data.Length * 2)
            throw new ArgumentException("destination is too short", nameof(destination));
        for (var i = 0; i < data.Length; i++)
        {
            var b = data[i];
            destination[i * 2] = NibbleLower[b >> 4];
            destination[i * 2 + 1] = NibbleLower[b & 0x0f];
        }
    }

    public static byte[] ReverseCopy(ReadOnlySpan<byte> data)
    {
        var copy = data.ToArray();
        Array.Reverse(copy);
        return copy;
    }

    public static bool TryParseU32Be(string? hex, out uint value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(hex))
            return false;
        var s = hex.AsSpan().Trim();
        if (s.Length >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
            s = s[2..];
        if (s.Length > 8)
            return false;
        // Pad left to 8 hex chars without allocating when already 8.
        if (s.Length == 8)
            return uint.TryParse(s, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out value);
        Span<char> padded = stackalloc char[8];
        padded.Fill('0');
        s.CopyTo(padded[(8 - s.Length)..]);
        return uint.TryParse(padded, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out value);
    }

    public static string U32BeHex(uint value) => value.ToString("x8");
}
