using System.Buffers;

namespace MiningcoreBtcSolo.Util;

/// <summary>
/// Short-lived pooled <see cref="IBufferWriter{T}"/> used to build UTF-8 protocol frames
/// without first materializing a UTF-16 string.
/// </summary>
internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;
    private int _written;

    public PooledByteBufferWriter(int initialCapacity = 256)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, initialCapacity));
    }

    public ReadOnlySpan<byte> WrittenSpan => Buffer.AsSpan(0, _written);
    public int WrittenCount => _written;

    public void Advance(int count)
    {
        if (count < 0 || _written > Buffer.Length - count)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return Buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return Buffer.AsSpan(_written);
    }

    private byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0)
            throw new ArgumentOutOfRangeException(nameof(sizeHint));
        if (sizeHint == 0)
            sizeHint = 1;
        if (sizeHint <= Buffer.Length - _written)
            return;

        var required = checked(_written + sizeHint);
        var nextSize = Math.Max(required, checked(Buffer.Length * 2));
        var next = ArrayPool<byte>.Shared.Rent(nextSize);
        Buffer.AsSpan(0, _written).CopyTo(next);
        ArrayPool<byte>.Shared.Return(Buffer);
        _buffer = next;
    }

    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer != null)
            ArrayPool<byte>.Shared.Return(buffer);
        _written = 0;
    }
}
