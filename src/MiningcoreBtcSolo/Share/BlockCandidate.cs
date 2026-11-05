using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Share;

/// <summary>
/// Immutable serialized block owned by the submit pipeline. The normal found-block
/// path keeps binary bytes and only materializes hexadecimal text for persistence.
/// </summary>
public sealed class BlockCandidate
{
    private readonly byte[] _bytes;
    private string? _hex;

    internal BlockCandidate(byte[] bytes)
    {
        if (bytes.Length < 81)
            throw new ArgumentException("serialized block is too short", nameof(bytes));
        _bytes = bytes;
    }

    public ReadOnlyMemory<byte> Bytes => _bytes;

    public string GetHex() => _hex ??= Hex.Encode(_bytes);
}
