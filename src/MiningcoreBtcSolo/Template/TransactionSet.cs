namespace MiningcoreBtcSolo.Template;

/// <summary>
/// Canonical transaction payloads stored in one contiguous buffer. Offsets preserve
/// transaction boundaries without retaining one managed array per transaction.
/// </summary>
public sealed class TransactionSet
{
    public static TransactionSet Empty { get; } =
        new(Array.Empty<byte>(), new[] { 0 });

    private readonly byte[] _payload;
    private readonly int[] _offsets;

    internal TransactionSet(byte[] payload, int[] offsets)
    {
        if (offsets.Length == 0 || offsets[0] != 0 || offsets[^1] != payload.Length)
            throw new ArgumentException("transaction offsets do not match payload", nameof(offsets));

        _payload = payload;
        _offsets = offsets;
    }

    public int Count => _offsets.Length - 1;
    public int SerializedLength => _payload.Length;
    public ReadOnlySpan<byte> SerializedBytes => _payload;

    public ReadOnlySpan<byte> GetTransaction(int index)
    {
        if ((uint)index >= (uint)Count)
            throw new ArgumentOutOfRangeException(nameof(index));

        var start = _offsets[index];
        return _payload.AsSpan(start, _offsets[index + 1] - start);
    }

    internal static TransactionSet CopyFrom(IReadOnlyList<byte[]> transactions)
    {
        if (transactions.Count == 0)
            return Empty;

        var offsets = new int[transactions.Count + 1];
        var totalBytes = 0;
        for (var i = 0; i < transactions.Count; i++)
        {
            var transaction = transactions[i] ??
                throw new ArgumentException("transaction cannot be null", nameof(transactions));
            if (transaction.Length == 0)
                throw new ArgumentException("transaction cannot be empty", nameof(transactions));
            offsets[i] = totalBytes;
            totalBytes = checked(totalBytes + transaction.Length);
        }
        offsets[^1] = totalBytes;

        var payload = GC.AllocateUninitializedArray<byte>(totalBytes);
        for (var i = 0; i < transactions.Count; i++)
            transactions[i].CopyTo(payload, offsets[i]);
        return new TransactionSet(payload, offsets);
    }
}

/// <summary>
/// Temporary contiguous LE txid storage used while fingerprinting and building
/// a template. Merkle construction consumes the buffer in place.
/// </summary>
internal sealed class TxIdSet
{
    private readonly byte[] _buffer;

    internal TxIdSet(int count)
    {
        Count = count;
        _buffer = count == 0
            ? Array.Empty<byte>()
            : GC.AllocateUninitializedArray<byte>(checked(count * 32));
    }

    public int Count { get; }
    public Span<byte> MutableBytes => _buffer;
    public ReadOnlySpan<byte> Bytes => _buffer;
}
