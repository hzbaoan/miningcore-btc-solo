using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MiningcoreBtcSolo.Rpc;

/// <summary>Decodes a JSON hex string directly into its final binary buffer.</summary>
internal sealed class HexByteArrayJsonConverter : JsonConverter<byte[]>
{
    public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("expected a hex string");
        if (reader.ValueIsEscaped)
        {
            var text = reader.GetString() ?? "";
            try
            {
                return Convert.FromHexString(text);
            }
            catch (FormatException ex)
            {
                throw new JsonException("invalid hex string", ex);
            }
        }

        var length = reader.HasValueSequence ? reader.ValueSequence.Length : reader.ValueSpan.Length;
        if ((length & 1) != 0 || length > int.MaxValue * 2L)
            throw new JsonException("hex string must have an even supported length");

        var result = new byte[(int)(length / 2)];
        if (!reader.HasValueSequence)
        {
            Decode(reader.ValueSpan, result);
            return result;
        }

        var source = new SequenceReader<byte>(reader.ValueSequence);
        for (var i = 0; i < result.Length; i++)
        {
            if (!source.TryRead(out var high) || !source.TryRead(out var low) ||
                !TryNibble(high, out var highNibble) || !TryNibble(low, out var lowNibble))
                throw new JsonException("invalid hex string");
            result[i] = (byte)((highNibble << 4) | lowNibble);
        }
        return result;
    }

    public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
    {
        var length = checked(value.Length * 2);
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(1, length));
        try
        {
            for (var i = 0; i < value.Length; i++)
            {
                var b = value[i];
                rented[i * 2] = ToHexNibble(b >> 4);
                rented[i * 2 + 1] = ToHexNibble(b & 0x0f);
            }
            writer.WriteStringValue(rented.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void Decode(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        for (var i = 0; i < destination.Length; i++)
        {
            if (!TryNibble(source[i * 2], out var high) || !TryNibble(source[i * 2 + 1], out var low))
                throw new JsonException("invalid hex string");
            destination[i] = (byte)((high << 4) | low);
        }
    }

    private static bool TryNibble(byte value, out byte nibble)
    {
        if (value is >= (byte)'0' and <= (byte)'9')
            nibble = (byte)(value - (byte)'0');
        else if (value is >= (byte)'a' and <= (byte)'f')
            nibble = (byte)(value - (byte)'a' + 10);
        else if (value is >= (byte)'A' and <= (byte)'F')
            nibble = (byte)(value - (byte)'A' + 10);
        else
        {
            nibble = 0;
            return false;
        }
        return true;
    }

    private static byte ToHexNibble(int value) =>
        (byte)(value < 10 ? '0' + value : 'a' + value - 10);
}
