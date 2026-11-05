namespace MiningcoreBtcSolo.Template;

public enum TemplateSource
{
    Startup,
    Longpoll,
    LongpollFallback,
    ZmqHashblock,
    ZmqRawblock,
    /// <summary>Empty clean job from P2P headers/cmpctblock only (solo).</summary>
    P2pFast,
    PostSubmit
}

public sealed class JobTemplate
{
    public bool Ready { get; init; }
    public string JobId { get; init; } = "0";
    /// <summary>Numeric form of the hexadecimal Stratum job id for allocation-free submit lookup.</summary>
    public ulong JobKey { get; init; }
    /// <summary>Monotonic process-local publication generation assigned by TemplateEngine.</summary>
    public long Epoch { get; internal set; }
    public bool SubmitOld { get; init; } = true;
    public string TemplateKey { get; init; } = "";
    public TemplateSource Source { get; init; }

    public uint Height { get; init; }
    public uint Version { get; init; }
    public uint Vbrequired { get; init; }
    public uint Nbits { get; init; }
    public uint Ntime { get; init; }
    public uint Mintime { get; init; }
    public long CoinbaseValue { get; init; }
    public double NetworkDifficulty { get; init; }

    public string PrevhashBe { get; init; } = "";
    public string PrevhashNotifyHex { get; init; } = "";
    public byte[] PrevhashLe { get; init; } = new byte[32];
    public byte[] TargetLe { get; init; } = new byte[32];

    public string VersionHex { get; init; } = "";
    public string NbitsHex { get; init; } = "";
    public string NtimeHex { get; init; } = "";

    public byte[] Coinbase1 { get; init; } = Array.Empty<byte>();
    public byte[] Coinbase2 { get; init; } = Array.Empty<byte>();
    public string Coinbase1Hex { get; init; } = "";
    public string Coinbase2Hex { get; init; } = "";

    public List<byte[]> MerkleBranchesLe { get; init; } = new();
    public List<string> MerkleBranchesHex { get; init; } = new();

    /// <summary>
    /// Full transaction payloads in one contiguous binary buffer for submitblock assembly.
    /// This avoids one long-lived managed array per transaction and roughly halves RAM
    /// versus retaining GBT hex strings.
    /// </summary>
    public TransactionSet Transactions { get; init; } = TransactionSet.Empty;
    public long TransactionBytes => Transactions.SerializedLength;

    public int TransactionCount => Transactions.Count;

    public bool HasWitnessCommitment { get; init; }
    public string? WitnessCommitmentScriptHex { get; init; }

    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    public static JobTemplate Empty() => new() { Ready = false };
}

public sealed class ChainTip
{
    public string HashHex { get; init; } = "";
    public uint Height { get; init; }
    public uint MedianTimePast { get; init; }
    public uint Nbits { get; init; }
    public uint Version { get; init; }
    public uint Vbrequired { get; init; }
    public byte[] TargetLe { get; init; } = new byte[32];
    public double NetworkDifficulty { get; init; }
}
