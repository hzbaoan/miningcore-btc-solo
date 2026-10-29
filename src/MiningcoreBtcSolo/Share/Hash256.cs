using System.Buffers.Binary;

namespace MiningcoreBtcSolo.Share;

/// <summary>A compact, allocation-free 256-bit hash stored in little-endian word order.</summary>
public readonly record struct Hash256(ulong Word0, ulong Word1, ulong Word2, ulong Word3)
{
    public static Hash256 FromLittleEndian(ReadOnlySpan<byte> value)
    {
        if (value.Length != 32)
            throw new ArgumentException("hash must be 32 bytes", nameof(value));

        return new Hash256(
            BinaryPrimitives.ReadUInt64LittleEndian(value),
            BinaryPrimitives.ReadUInt64LittleEndian(value[8..]),
            BinaryPrimitives.ReadUInt64LittleEndian(value[16..]),
            BinaryPrimitives.ReadUInt64LittleEndian(value[24..]));
    }

    public void WriteLittleEndian(Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("destination must be at least 32 bytes", nameof(destination));

        BinaryPrimitives.WriteUInt64LittleEndian(destination, Word0);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], Word1);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[16..], Word2);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[24..], Word3);
    }

    /// <summary>Encode the conventional display form (big-endian, lowercase hex).</summary>
    public string ToHex() => string.Create(64, this, static (chars, hash) =>
    {
        for (var i = 0; i < 32; i++)
        {
            var value = hash.GetLittleEndianByte(31 - i);
            chars[i * 2] = ToHexNibble(value >> 4);
            chars[i * 2 + 1] = ToHexNibble(value & 0x0f);
        }
    });

    public override string ToString() => ToHex();

    private byte GetLittleEndianByte(int index)
    {
        var word = index switch
        {
            < 8 => Word0,
            < 16 => Word1,
            < 24 => Word2,
            _ => Word3
        };
        return (byte)(word >> ((index & 7) * 8));
    }

    private static char ToHexNibble(int value) =>
        (char)(value < 10 ? '0' + value : 'a' + value - 10);
}
