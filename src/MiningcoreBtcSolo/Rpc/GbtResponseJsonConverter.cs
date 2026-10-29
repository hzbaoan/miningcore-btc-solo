using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Rpc;

/// <summary>
/// Materializes Bitcoin Core GBT directly into packed transaction/txid buffers.
/// This avoids one object, two hash strings, and one byte array per transaction.
/// </summary>
internal sealed class GbtResponseJsonConverter : JsonConverter<GbtResponse>
{
    public override GbtResponse Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("GBT result must be an object");

        var result = new GbtResponse();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return result;
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("invalid GBT property");

            var property = reader.GetString();
            if (!reader.Read())
                throw new JsonException("truncated GBT property");

            switch (property)
            {
                case "version":
                    result.Version = reader.GetUInt32();
                    break;
                case "previousblockhash":
                    result.PreviousBlockhash = reader.GetString() ?? "";
                    break;
                case "coinbasevalue":
                    result.CoinbaseValue = reader.GetInt64();
                    break;
                case "target":
                    result.Target = reader.GetString() ?? "";
                    break;
                case "curtime":
                    result.CurTime = reader.GetUInt32();
                    break;
                case "bits":
                    result.Bits = reader.GetString() ?? "";
                    break;
                case "height":
                    result.Height = reader.GetUInt32();
                    break;
                case "transactions":
                    result.PackedTransactions = ReadTransactions(ref reader);
                    break;
                case "coinbaseaux":
                    result.CoinbaseAux = ReadCoinbaseAux(ref reader);
                    break;
                case "default_witness_commitment":
                    result.DefaultWitnessCommitment = ReadNullableString(ref reader);
                    break;
                case "longpollid":
                    result.LongPollId = ReadNullableString(ref reader);
                    break;
                case "mintime":
                    result.Mintime = reader.TokenType == JsonTokenType.Null
                        ? null
                        : reader.GetUInt32();
                    break;
                case "vbrequired":
                    result.Vbrequired = reader.GetUInt32();
                    break;
                case "submitold":
                    result.SubmitOld = reader.TokenType == JsonTokenType.Null
                        ? null
                        : reader.GetBoolean();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        throw new JsonException("truncated GBT result");
    }

    public override void Write(
        Utf8JsonWriter writer,
        GbtResponse value,
        JsonSerializerOptions options)
    {
        var transactions = value.Transactions ?? Array.Empty<GbtTx>();
        if (value.PackedTransactions != null && transactions.Length == 0 &&
            value.PackedTransactions.Transactions.Count != 0)
        {
            throw new NotSupportedException(
                "serializing a packed GBT response would require unavailable wtxid fields");
        }

        writer.WriteStartObject();
        writer.WriteNumber("version", value.Version);
        writer.WriteString("previousblockhash", value.PreviousBlockhash);
        writer.WriteNumber("coinbasevalue", value.CoinbaseValue);
        writer.WriteString("target", value.Target);
        writer.WriteNumber("curtime", value.CurTime);
        writer.WriteString("bits", value.Bits);
        writer.WriteNumber("height", value.Height);
        writer.WritePropertyName("transactions");
        writer.WriteStartArray();
        foreach (var transaction in transactions)
        {
            writer.WriteStartObject();
            writer.WriteString("data", Hex.Encode(transaction.Data));
            if (transaction.TxId != null)
                writer.WriteString("txid", transaction.TxId);
            if (transaction.Hash != null)
                writer.WriteString("hash", transaction.Hash);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        if (value.CoinbaseAux != null)
        {
            writer.WritePropertyName("coinbaseaux");
            writer.WriteStartObject();
            if (value.CoinbaseAux.Flags != null)
                writer.WriteString("flags", value.CoinbaseAux.Flags);
            writer.WriteEndObject();
        }
        if (value.DefaultWitnessCommitment != null)
            writer.WriteString("default_witness_commitment", value.DefaultWitnessCommitment);
        if (value.LongPollId != null)
            writer.WriteString("longpollid", value.LongPollId);
        if (value.Mintime.HasValue)
            writer.WriteNumber("mintime", value.Mintime.Value);
        writer.WriteNumber("vbrequired", value.Vbrequired);
        if (value.SubmitOld.HasValue)
            writer.WriteBoolean("submitold", value.SubmitOld.Value);
        writer.WriteEndObject();
    }

    private static GbtPackedTransactions ReadTransactions(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException("GBT transactions must be an array");

        using var payload = new PooledByteBufferWriter(64 * 1024);
        using var txids = new PooledByteBufferWriter(32 * 1024);
        var offsets = new List<int> { 0 };
        var hasWitness = false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                if (offsets.Count == 1)
                    return GbtPackedTransactions.Empty;

                return new GbtPackedTransactions(
                    new TransactionSet(payload.WrittenSpan.ToArray(), offsets.ToArray()),
                    txids.WrittenSpan.ToArray(),
                    hasWitness);
            }
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException("GBT transaction must be an object");

            ReadTransaction(ref reader, payload, txids, offsets, ref hasWitness);
        }

        throw new JsonException("truncated GBT transactions");
    }

    private static void ReadTransaction(
        ref Utf8JsonReader reader,
        PooledByteBufferWriter payload,
        PooledByteBufferWriter txids,
        List<int> offsets,
        ref bool hasWitness)
    {
        var transactionStart = payload.WrittenCount;
        var hasData = false;
        var hasTxid = false;
        Span<byte> txidBe = stackalloc byte[32];

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                if (!hasData)
                    throw new JsonException("GBT transaction missing data");
                if (!hasTxid)
                    throw new JsonException("GBT transaction missing txid");

                var transactionLength = payload.WrittenCount - transactionStart;
                if (transactionLength >= 6)
                {
                    var bytes = payload.WrittenSpan;
                    hasWitness |= bytes[transactionStart + 4] == 0 &&
                        bytes[transactionStart + 5] != 0;
                }

                var txidDestination = txids.GetSpan(32);
                for (var i = 0; i < 32; i++)
                    txidDestination[i] = txidBe[31 - i];
                txids.Advance(32);
                offsets.Add(payload.WrittenCount);
                return;
            }
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("invalid GBT transaction property");

            var isData = reader.ValueTextEquals("data"u8);
            var isTxid = reader.ValueTextEquals("txid"u8);
            if (!reader.Read())
                throw new JsonException("truncated GBT transaction property");

            if (isData)
            {
                if (hasData)
                    throw new JsonException("GBT transaction has duplicate data");
                DecodeHexToWriter(ref reader, payload, requireBytes: false);
                if (payload.WrittenCount == transactionStart)
                    throw new JsonException("GBT transaction data is empty");
                hasData = true;
            }
            else if (isTxid)
            {
                if (hasTxid)
                    throw new JsonException("GBT transaction has duplicate txid");
                DecodeFixedHex(ref reader, txidBe);
                hasTxid = true;
            }
            else
            {
                reader.Skip();
            }
        }

        throw new JsonException("truncated GBT transaction");
    }

    private static GbtCoinbaseAux? ReadCoinbaseAux(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("coinbaseaux must be an object");

        var aux = new GbtCoinbaseAux();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return aux;
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("invalid coinbaseaux property");

            var property = reader.GetString();
            if (!reader.Read())
                throw new JsonException("truncated coinbaseaux property");
            if (property == "flags")
                aux.Flags = ReadNullableString(ref reader);
            else
                reader.Skip();
        }

        throw new JsonException("truncated coinbaseaux");
    }

    private static string? ReadNullableString(ref Utf8JsonReader reader) =>
        reader.TokenType == JsonTokenType.Null ? null : reader.GetString();

    private static void DecodeFixedHex(
        scoped ref Utf8JsonReader reader,
        scoped Span<byte> destination)
    {
        var length = GetHexLength(ref reader);
        if (length != destination.Length * 2)
            throw new JsonException($"hex value must be exactly {destination.Length} bytes");
        DecodeHex(ref reader, destination);
    }

    private static void DecodeHexToWriter(
        scoped ref Utf8JsonReader reader,
        PooledByteBufferWriter destination,
        bool requireBytes)
    {
        var length = GetHexLength(ref reader);
        if ((length & 1) != 0 || length > int.MaxValue * 2L)
            throw new JsonException("hex value has an invalid length");
        var byteLength = checked((int)(length / 2));
        if (requireBytes && byteLength == 0)
            throw new JsonException("hex value is empty");

        var output = destination.GetSpan(byteLength);
        DecodeHex(ref reader, output[..byteLength]);
        destination.Advance(byteLength);
    }

    private static long GetHexLength(scoped ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("expected a hex string");
        if (reader.ValueIsEscaped)
            return (reader.GetString() ?? "").Length;
        return reader.HasValueSequence ? reader.ValueSequence.Length : reader.ValueSpan.Length;
    }

    private static void DecodeHex(
        scoped ref Utf8JsonReader reader,
        scoped Span<byte> destination)
    {
        if (reader.ValueIsEscaped)
        {
            var text = reader.GetString() ?? "";
            byte[] decoded;
            try
            {
                decoded = Convert.FromHexString(text);
            }
            catch (FormatException ex)
            {
                throw new JsonException("invalid hex string", ex);
            }
            if (decoded.Length != destination.Length)
                throw new JsonException("hex value length changed after unescaping");
            decoded.CopyTo(destination);
            return;
        }

        if (!reader.HasValueSequence)
        {
            DecodeHexSpan(reader.ValueSpan, destination);
            return;
        }

        var source = new SequenceReader<byte>(reader.ValueSequence);
        for (var i = 0; i < destination.Length; i++)
        {
            if (!source.TryRead(out var high) || !source.TryRead(out var low) ||
                !TryNibble(high, out var highNibble) || !TryNibble(low, out var lowNibble))
                throw new JsonException("invalid hex string");
            destination[i] = (byte)((highNibble << 4) | lowNibble);
        }
    }

    private static void DecodeHexSpan(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        for (var i = 0; i < destination.Length; i++)
        {
            if (!TryNibble(source[i * 2], out var high) ||
                !TryNibble(source[i * 2 + 1], out var low))
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
}

internal sealed class GbtPackedTransactions
{
    public static GbtPackedTransactions Empty { get; } =
        new(TransactionSet.Empty, Array.Empty<byte>(), false);

    public GbtPackedTransactions(
        TransactionSet transactions,
        byte[] txidsLe,
        bool hasWitnessTransactions)
    {
        if (txidsLe.Length != transactions.Count * 32)
            throw new ArgumentException("txid count does not match transactions", nameof(txidsLe));
        Transactions = transactions;
        TxidsLe = txidsLe;
        HasWitnessTransactions = hasWitnessTransactions;
    }

    public TransactionSet Transactions { get; }
    public byte[] TxidsLe { get; }
    public bool HasWitnessTransactions { get; }

    public TxIdSet CopyTxids()
    {
        var result = new TxIdSet(Transactions.Count);
        TxidsLe.CopyTo(result.MutableBytes);
        return result;
    }
}
