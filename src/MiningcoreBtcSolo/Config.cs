using System.Text.Json;
using System.Text.Json.Serialization;
using NBitcoin;

namespace MiningcoreBtcSolo;

public sealed class AppConfig
{
    /// <summary>Console filter: Debug | Information | Warning | Error | None (Alert always prints).</summary>
    [JsonPropertyName("log_level")]
    public string LogLevel { get; set; } = "Information";

    [JsonPropertyName("network")]
    public string NetworkName { get; set; } = "mainnet";

    [JsonPropertyName("stratum")]
    public StratumConfig Stratum { get; set; } = new();

    [JsonPropertyName("bitcoind")]
    public BitcoindConfig Bitcoind { get; set; } = new();

    [JsonPropertyName("coinbase")]
    public CoinbaseConfig Coinbase { get; set; } = new();

    [JsonPropertyName("difficulty")]
    public DifficultyConfig Difficulty { get; set; } = new();

    [JsonPropertyName("runtime")]
    public RuntimeConfig Runtime { get; set; } = new();

    [JsonPropertyName("api")]
    public ApiConfig Api { get; set; } = new();

    [JsonIgnore]
    public Network Network { get; private set; } = Network.Main;

    public static AppConfig Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Config not found: {path}");

        var json = File.ReadAllText(path);
        var cfg = JsonSerializer.Deserialize<AppConfig>(json, JsonOptions())
                  ?? throw new InvalidOperationException("Failed to parse config.json");
        cfg.Validate();
        return cfg;
    }

    public void Validate()
    {
        Network = NetworkName.ToLowerInvariant() switch
        {
            "mainnet" or "bitcoin" => Network.Main,
            "testnet" => Network.TestNet,
            "regtest" => Network.RegTest,
            _ => throw new InvalidOperationException($"Unsupported network: {NetworkName}")
        };

        if (string.IsNullOrWhiteSpace(Bitcoind.RpcUrl))
            throw new InvalidOperationException("bitcoind.rpc_url is required");
        if (string.IsNullOrWhiteSpace(Bitcoind.RpcUser) || string.IsNullOrWhiteSpace(Bitcoind.RpcPassword))
            throw new InvalidOperationException("bitcoind.rpc_user and rpc_password are required");
        if (string.IsNullOrWhiteSpace(Coinbase.Address))
            throw new InvalidOperationException("coinbase.address is required");
        if (IsPlaceholder(Bitcoind.RpcUser) || IsPlaceholder(Bitcoind.RpcPassword) ||
            IsPlaceholder(Coinbase.Address))
            throw new InvalidOperationException(
                "config contains REPLACE_ placeholder values; set RPC credentials and coinbase.address");

        // Fixed coinbase destination - fail fast if address/network mismatch.
        _ = BitcoinAddress.Create(Coinbase.Address, Network);

        if (Stratum.Extranonce1Size is < 1 or > 4)
            throw new InvalidOperationException("stratum.extranonce1_size must be 1..4");
        if (Stratum.Extranonce2Size is < 1 or > 8)
            throw new InvalidOperationException("stratum.extranonce2_size must be 1..8");
        var extranonce1Capacity = 1L << (Stratum.Extranonce1Size * 8);
        if (Stratum.MaxConnections < 1 || Stratum.MaxConnections > extranonce1Capacity)
        {
            throw new InvalidOperationException(
                "stratum.max_connections must fit the extranonce1 keyspace");
        }
        if (Stratum.MaxMessageBytes is < 1024 or > 1024 * 1024)
            throw new InvalidOperationException("stratum.max_message_bytes must be 1024..1048576");
        if (Stratum.SendQueueCapacity is < 4 or > 4096)
            throw new InvalidOperationException("stratum.send_queue_capacity must be 4..4096");
        if (Stratum.WriteTimeoutSecs is < 1 or > 300)
            throw new InvalidOperationException("stratum.write_timeout_secs must be 1..300");
        if (Stratum.CleanBroadcastTimeoutMs is < 100 or > 30_000)
            throw new InvalidOperationException("stratum.clean_broadcast_timeout_ms must be 100..30000");
        if (Stratum.LateShareGraceMs is < 0 or > 30_000)
            throw new InvalidOperationException("stratum.late_share_grace_ms must be 0..30000");
        if (Runtime.KeepOldJobs is < 0 or > 64)
            throw new InvalidOperationException("runtime.keep_old_jobs must be 0..64");
        if (Runtime.MaxRetiredJobs is < 1 or > 64)
            throw new InvalidOperationException("runtime.max_retired_jobs must be 1..64");
        if (Runtime.RetiredJobMaxAgeSecs is < 1 or > 300)
            throw new InvalidOperationException("runtime.retired_job_max_age_secs must be 1..300");
        if (Runtime.MaxRetainedTransactionBytes is < 0 or > 16L * 1024 * 1024 * 1024)
            throw new InvalidOperationException("runtime.max_retained_transaction_bytes must be 0..17179869184");
        if (!string.IsNullOrWhiteSpace(Api.P2pFastPeerUuid) &&
            !Guid.TryParse(Api.P2pFastPeerUuid, out _))
            throw new InvalidOperationException("api.p2p_fast_peer_uuid must be a UUID when configured");
        if (!double.IsFinite(Difficulty.Min) || !double.IsFinite(Difficulty.Max) ||
            Difficulty.Min <= 0 || Difficulty.Max < Difficulty.Min ||
            Difficulty.Max > Util.BitcoinEncoding.MaxSupportedShareDifficulty)
            throw new InvalidOperationException("invalid difficulty bounds");
        if (!double.IsFinite(Difficulty.Default))
            throw new InvalidOperationException("difficulty.default must be finite");
        if (Difficulty.Default < Difficulty.Min || Difficulty.Default > Difficulty.Max)
            Difficulty.Default = Math.Clamp(Difficulty.Default, Difficulty.Min, Difficulty.Max);
        if (Difficulty.TargetTimeSecs <= 0)
            Difficulty.TargetTimeSecs = 5;
        if (Difficulty.RetargetTimeSecs < 1)
            Difficulty.RetargetTimeSecs = 1;
        if (Difficulty.RetargetShareBurst < 2)
            Difficulty.RetargetShareBurst = 2;
        if (!double.IsFinite(Difficulty.VariancePercent) || Difficulty.VariancePercent is < 0 or > 90)
            Difficulty.VariancePercent = 30;
        if (!double.IsFinite(Difficulty.RetargetSmoothing) || Difficulty.RetargetSmoothing is < 0.05 or > 1)
            Difficulty.RetargetSmoothing = 0.25;
        if (Difficulty.MaxStepUp < 1.1)
            Difficulty.MaxStepUp = 1.1;
        if (Difficulty.MaxStepUpBurst < Difficulty.MaxStepUp)
            Difficulty.MaxStepUpBurst = Difficulty.MaxStepUp;
        if (Difficulty.MaxStepDown is <= 0 or >= 1)
            Difficulty.MaxStepDown = 0.5;
    }

    private static bool IsPlaceholder(string value) =>
        value.StartsWith("REPLACE_", StringComparison.OrdinalIgnoreCase);

    public static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
        WriteIndented = true
    };
}

public sealed class StratumConfig
{
    [JsonPropertyName("listen_addr")] public string ListenAddr { get; set; } = "127.0.0.1";
    [JsonPropertyName("listen_port")] public int ListenPort { get; set; } = 3333;
    [JsonPropertyName("extranonce1_size")] public int Extranonce1Size { get; set; } = 4;
    [JsonPropertyName("extranonce2_size")] public int Extranonce2Size { get; set; } = 4;
    /// <summary>
    /// Drop connections that do not complete subscribe/authorize within this many
    /// seconds (0 = disabled). Authorized miners are exempt because Stratum V1 has
    /// no mandatory client heartbeat.
    /// </summary>
    [JsonPropertyName("idle_timeout_secs")] public int IdleTimeoutSecs { get; set; } = 3600;
    [JsonPropertyName("max_connections")] public int MaxConnections { get; set; } = 256;
    /// <summary>Maximum UTF-8 bytes in one newline-delimited Stratum request.</summary>
    [JsonPropertyName("max_message_bytes")] public int MaxMessageBytes { get; set; } = 64 * 1024;
    /// <summary>Per-client bounded outbound frame queue. Full queues identify slow clients.</summary>
    [JsonPropertyName("send_queue_capacity")] public int SendQueueCapacity { get; set; } = 64;
    /// <summary>Maximum time allowed for one socket write before the client is disconnected.</summary>
    [JsonPropertyName("write_timeout_secs")] public int WriteTimeoutSecs { get; set; } = 10;
    /// <summary>Maximum time for a clean job to reach each live client's TCP stack.</summary>
    [JsonPropertyName("clean_broadcast_timeout_ms")] public int CleanBroadcastTimeoutMs { get; set; } = 1500;
    /// <summary>Keep retired jobs after clean delivery for shares already in flight.</summary>
    [JsonPropertyName("late_share_grace_ms")] public int LateShareGraceMs { get; set; } = 2000;
}

public sealed class BitcoindConfig
{
    [JsonPropertyName("rpc_url")] public string RpcUrl { get; set; } = "http://127.0.0.1:8332";
    [JsonPropertyName("rpc_user")] public string RpcUser { get; set; } = "";
    [JsonPropertyName("rpc_password")] public string RpcPassword { get; set; } = "";
    [JsonPropertyName("zmq_block_urls")] public List<string> ZmqBlockUrls { get; set; } = new();
    /// <summary>Single configured P2P-fast peer. It stays active alongside runtime-managed peers.</summary>
    [JsonPropertyName("p2p_fast_peer")] public string? P2pFastPeer { get; set; }
}

public sealed class CoinbaseConfig
{
    [JsonPropertyName("address")] public string Address { get; set; } = "";
    [JsonPropertyName("message")] public string Message { get; set; } = "mcore-solo";
    [JsonPropertyName("segwit_commitment")] public bool SegwitCommitment { get; set; } = true;
}

public sealed class DifficultyConfig
{
    [JsonPropertyName("min")] public double Min { get; set; } = 1024;
    /// <summary>Upper clamp. Large farms need high max (default 1e12 ≈ multi-PH at ~5s/share).</summary>
    [JsonPropertyName("max")] public double Max { get; set; } = 1e12;
    [JsonPropertyName("default")] public double Default { get; set; } = 8192;
    /// <summary>Desired mean time between accepted shares per connection.</summary>
    [JsonPropertyName("target_time_secs")] public double TargetTimeSecs { get; set; } = 5;
    /// <summary>Steady-state retarget interval when share rate is normal.</summary>
    [JsonPropertyName("retarget_time_secs")] public double RetargetTimeSecs { get; set; } = 30;
    /// <summary>
    /// Early retarget: if this many accepted shares arrive before retarget_time_secs,
    /// raise difficulty immediately (flood / big miner catch-up).
    /// </summary>
    [JsonPropertyName("retarget_share_burst")] public int RetargetShareBurst { get; set; } = 8;
    /// <summary>Allowed steady share-interval variance before changing difficulty.</summary>
    [JsonPropertyName("variance_percent")] public double VariancePercent { get; set; } = 30;
    /// <summary>EWMA weight of the newest completed window (0.05..1); lower is steadier.</summary>
    [JsonPropertyName("retarget_smoothing")] public double RetargetSmoothing { get; set; } = 0.25;
    /// <summary>Max × multiplier per steady retarget (default 2×).</summary>
    [JsonPropertyName("max_step_up")] public double MaxStepUp { get; set; } = 2;
    /// <summary>Max × multiplier when burst/flood path fires (default 32×).</summary>
    [JsonPropertyName("max_step_up_burst")] public double MaxStepUpBurst { get; set; } = 32;
    /// <summary>Max × multiplier downward per retarget.</summary>
    [JsonPropertyName("max_step_down")] public double MaxStepDown { get; set; } = 0.5;
}

public sealed class RuntimeConfig
{
    /// <summary>
    /// How many previous jobs to retain for late submits (plus current).
    /// Lower = less RAM (each job may hold a full GBT tx set). Default 3 is enough for solo.
    /// </summary>
    [JsonPropertyName("keep_old_jobs")] public int KeepOldJobs { get; set; } = 3;

    /// <summary>Hard memory bound for clean-retired templates awaiting safe reclamation.</summary>
    [JsonPropertyName("max_retired_jobs")] public int MaxRetiredJobs { get; set; } = 8;

    /// <summary>Hard safety age for retired full templates if a delivery barrier is lost.</summary>
    [JsonPropertyName("retired_job_max_age_secs")] public int RetiredJobMaxAgeSecs { get; set; } = 15;

    /// <summary>Byte budget for retained transaction payloads; the active job is always retained.</summary>
    [JsonPropertyName("max_retained_transaction_bytes")]
    public long MaxRetainedTransactionBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>
    /// Directory for durable pending block submissions (relative paths resolve under process CWD).
    /// Default: data/
    /// </summary>
    [JsonPropertyName("data_dir")] public string DataDir { get; set; } = "data";
}

public sealed class ApiConfig
{
    [JsonPropertyName("listen_addr")] public string ListenAddr { get; set; } = "127.0.0.1";
    [JsonPropertyName("listen_port")] public int ListenPort { get; set; } = 7152;
    [JsonPropertyName("enabled")] public bool Enabled { get; set; } = true;
    /// <summary>UUID that enables the P2P-fast peer management page.</summary>
    [JsonPropertyName("p2p_fast_peer_uuid")] public string? P2pFastPeerUuid { get; set; }
}
