using System.Diagnostics;

namespace MiningcoreBtcSolo.Metrics;

/// <summary>
/// In-memory metrics. Hashrate uses a non-destructive sliding window of accepted shares:
///   H/s = sum(assigned_difficulty) * 2^32 / window_seconds
/// Workers are keyed by Stratum session id (one TCP connection = one worker row).
/// Public API DTOs only expose extranonce1 + user-agent (no BTC address, IP, or session id).
/// </summary>
public sealed class MetricsStore
{
    /// <summary>Rolling window for hashrate (10 minutes).</summary>
    public const long HashrateWindowMs = 600_000;

    /// <summary>Warm-up: do not use a longer denominator than observed span (+ small floor).</summary>
    public const long HashrateMinWindowMs = 15_000;

    /// <summary>Recent accepted shares kept for dashboard (/api/shares). Older entries are dropped.</summary>
    private const int MaxShareEvents = 15;
    private const int MaxBlockEvents = 32;
    private const long HashrateBucketMs = 5_000;
    internal const int HashrateBucketCount =
        (int)(HashrateWindowMs / HashrateBucketMs) + 1;

    /// <summary>Bitcoin stratum: one difficulty-1 share ≈ 2^32 hashes expected.</summary>
    private const double HashesPerDifficulty = 4294967296.0; // 2^32

    private readonly object _gate = new();
    private readonly Dictionary<string, WorkerStats> _workers = new();
    private readonly ShareSnapshot[] _shares = new ShareSnapshot[MaxShareEvents];
    private readonly LinkedList<BlockEvent> _blocks = new();
    private readonly HashrateBucket[] _totalHashrateBuckets = new HashrateBucket[HashrateBucketCount];
    private readonly DateTimeOffset _started = DateTimeOffset.UtcNow;
    private readonly Func<long>? _utcNowMilliseconds;
    private int _shareWriteIndex;
    private int _shareCount;

    public MetricsStore()
    {
    }

    internal MetricsStore(Func<long> utcNowMilliseconds)
    {
        _utcNowMilliseconds = utcNowMilliseconds;
    }

    /// <summary>Dashboard GetWorkers snapshot TTL (avoid O(workers×samples) every poll).</summary>
    private const long WorkersCacheTtlMs = 1_000;
    private List<WorkerDto>? _workersCache;
    private long _workersCacheMs;
    private long _workersVersion;

    public long SharesValid;
    public long SharesError;
    public long SharesLate;
    public long SharesStale;
    public long SharesUnknownJob;
    public long CleanBroadcasts;
    public long CleanBroadcastClientTimeouts;
    public long BlocksSubmitted;
    public long BlocksAccepted;
    public int Connections;
    public int Subscriptions;

    public ShareEvent? BestShare { get; private set; }
    public bool LastRefreshOk { get; set; }
    public long LastRefreshMs { get; set; }
    public double NetworkHashrateHps { get; set; }

    private long _shareValidationSamples;
    private long _shareValidationTotalTicks;
    private long _shareValidationMaxTicks;
    private long _acceptedAckQueued;
    private long _acceptedAckQueueTotalTicks;
    private long _acceptedAckQueueMaxTicks;
    private long _acceptedAckWritten;
    private long _acceptedAckWriteTotalTicks;
    private long _acceptedAckWriteMaxTicks;

    public long UptimeSeconds => (long)(DateTimeOffset.UtcNow - _started).TotalSeconds;

    public void SetConnections(int n) => Connections = n;
    public void SetSubscriptions(int n) => Subscriptions = n;

    /// <summary>
    /// Drop a disconnected Stratum session from the worker map.
    /// Hashrate samples age out via the sliding window; BestShare is retained for display.
    /// </summary>
    public void RemoveSession(string sessionId)
    {
        if (string.IsNullOrWhiteSpace(sessionId))
            return;
        // Worker list changed — drop dashboard cache.
        lock (_gate)
        {
            _workers.Remove(sessionId.Trim());
            _workersVersion++;
            _workersCache = null;
            _workersCacheMs = 0;
        }
    }

    /// <summary>Reject counter only — no identity / no lock (share flood safe).</summary>
    public void RecordShareError()
    {
        Interlocked.Increment(ref SharesError);
    }

    /// <summary>
    /// Accepted share: one lock for event + worker upsert + hashrate sample + assigned difficulty.
    /// Pass <paramref name="assignedDifficulty"/> to refresh worker row (replaces a separate TouchWorker).
    /// </summary>
    public void RecordShare(
        WorkerIdentity identity,
        double creditDiff,
        double actualDiff,
        bool accepted,
        double? assignedDifficulty = null,
        string hash = "")
    {
        if (!accepted)
        {
            Interlocked.Increment(ref SharesError);
            return;
        }

        Interlocked.Increment(ref SharesValid);
        // Prefer pre-normalized session identity (Stratum caches it) — skip second allocation.
        var id = identity.IsNormalized ? identity : identity.Normalize();
        var nowMs = UtcNowMilliseconds();

        lock (_gate)
        {
            _shares[_shareWriteIndex] = new ShareSnapshot(
                id.UserAgent,
                id.Extranonce1,
                hash,
                creditDiff,
                actualDiff,
                nowMs);
            _shareWriteIndex++;
            if (_shareWriteIndex == MaxShareEvents)
                _shareWriteIndex = 0;
            if (_shareCount < MaxShareEvents)
                _shareCount++;

            if (BestShare == null || actualDiff > BestShare.ActualDiff)
            {
                BestShare = new ShareEvent
                {
                    SessionId = id.SessionId,
                    Worker = id.Name,
                    UserAgent = id.UserAgent,
                    Peer = id.Peer,
                    Extranonce1 = id.Extranonce1,
                    Difficulty = creditDiff,
                    ActualDiff = actualDiff,
                    TimestampMs = nowMs
                };
            }

            var w = UpsertWorkerLocked(id, nowMs);
            w.BestDiff = Math.Max(w.BestDiff, actualDiff);
            w.LastShareMs = nowMs;
            if (assignedDifficulty.HasValue)
                w.AssignedDifficulty = assignedDifficulty.Value;

            if (creditDiff > 0)
            {
                AddHashrateSample(_totalHashrateBuckets, nowMs, creditDiff);
                AddHashrateSample(w.HashrateBuckets, nowMs, creditDiff);
            }
        }
    }

    public void TouchWorker(WorkerIdentity identity, double difficulty)
    {
        var id = identity.IsNormalized ? identity : identity.Normalize();
        var nowMs = UtcNowMilliseconds();
        lock (_gate)
        {
            var w = UpsertWorkerLocked(id, nowMs);
            w.AssignedDifficulty = difficulty;
        }
    }

    public void RecordBlock(uint height, string hash, string result)
    {
        lock (_gate)
        {
            var existingNode = _blocks.First;
            while (existingNode != null &&
                   !string.Equals(existingNode.Value.Hash, hash, StringComparison.OrdinalIgnoreCase))
                existingNode = existingNode.Next;

            var accepted = IsAcceptedBlockResult(result);
            if (existingNode != null)
            {
                var wasAccepted = IsAcceptedBlockResult(existingNode.Value.Result);
                existingNode.Value.Height = height;
                existingNode.Value.Result = result;
                existingNode.Value.TimestampMs = UtcNowMilliseconds();
                _blocks.Remove(existingNode);
                _blocks.AddFirst(existingNode);
                if (accepted && !wasAccepted)
                    Interlocked.Increment(ref BlocksAccepted);
                return;
            }

            Interlocked.Increment(ref BlocksSubmitted);
            if (accepted)
                Interlocked.Increment(ref BlocksAccepted);
            _blocks.AddFirst(new BlockEvent
            {
                Height = height,
                Hash = hash,
                Result = result,
                TimestampMs = UtcNowMilliseconds()
            });
            while (_blocks.Count > MaxBlockEvents)
                _blocks.RemoveLast();
        }
    }

    private static bool IsAcceptedBlockResult(string result) =>
        result is "submitted" or "duplicate" or "accepted";

    /// <summary>Total pool hashrate in H/s over the sliding window (read-only; no decay).</summary>
    public double EstimateTotalHps()
    {
        lock (_gate)
        {
            var now = UtcNowMilliseconds();
            var aggregate = AggregateHashrateBuckets(_totalHashrateBuckets, now);
            return ComputeHashrateHps(aggregate.SumDifficulty, aggregate.OldestTimestampMs, now);
        }
    }

    public List<WorkerDto> GetWorkers()
    {
        var now = UtcNowMilliseconds();
        WorkerSnapshot[] workers;
        long workersVersion;
        lock (_gate)
        {
            if (_workersCache != null && now - _workersCacheMs < WorkersCacheTtlMs)
                return _workersCache;

            workersVersion = _workersVersion;
            workers = _workers.Values
                .Where(w => now - w.LastSeenMs < 600_000)
                .Select(w =>
                {
                    var aggregate = AggregateHashrateBuckets(w.HashrateBuckets, now);
                    return new WorkerSnapshot(
                        w.UserAgent,
                        w.Extranonce1,
                        w.BestDiff,
                        w.AssignedDifficulty,
                        w.LastShareMs,
                        aggregate.SumDifficulty,
                        aggregate.OldestTimestampMs);
                })
                .ToArray();
        }

        // Privacy: never expose BTC address (name), IP (peer), or session_id on the API.
        var result = workers
            .Select(w =>
            {
                var hps = ComputeHashrateHps(w.SumDifficulty, w.OldestTimestampMs, now);
                return new WorkerDto
                {
                    user_agent = w.UserAgent,
                    extranonce1 = w.Extranonce1,
                    best_diff = w.BestDiff,
                    hashrate = hps / 1e12,
                    hashrate_hps = hps,
                    assigned_difficulty = w.AssignedDifficulty,
                    last_share_ms = w.LastShareMs
                };
            })
            .OrderByDescending(w => w.hashrate_hps)
            .ThenBy(w => w.extranonce1, StringComparer.OrdinalIgnoreCase)
            .ThenBy(w => w.user_agent, StringComparer.OrdinalIgnoreCase)
            .ToList();

        lock (_gate)
        {
            if (_workersCache != null && _workersCacheMs >= now)
                return _workersCache;
            if (_workersVersion == workersVersion)
            {
                _workersCache = result;
                _workersCacheMs = now;
            }
            return result;
        }
    }

    public List<ShareDto> GetShares()
    {
        lock (_gate)
        {
            // Newest-first; cap matches MaxShareEvents (dashboard shows last 15).
            // Privacy: only share hash, en1, and UA are public (peer stays in process logs).
            var result = new List<ShareDto>(_shareCount);
            var index = _shareWriteIndex;
            for (var i = 0; i < _shareCount; i++)
            {
                if (--index < 0)
                    index = MaxShareEvents - 1;
                ref readonly var share = ref _shares[index];
                result.Add(new ShareDto
                {
                    user_agent = share.UserAgent,
                    extranonce1 = share.Extranonce1,
                    hash = share.Hash,
                    difficulty = share.Difficulty,
                    actual_diff = share.ActualDiff,
                    timestamp_ms = share.TimestampMs
                });
            }
            return result;
        }
    }

    public List<BlockDto> GetBlocks()
    {
        lock (_gate)
        {
            return _blocks.Select(b => new BlockDto
            {
                height = b.Height,
                hash = b.Hash,
                result = b.Result,
                timestamp_ms = b.TimestampMs
            }).ToList();
        }
    }

    private WorkerStats UpsertWorkerLocked(WorkerIdentity id, long nowMs)
    {
        if (!_workers.TryGetValue(id.SessionId, out var w))
        {
            w = new WorkerStats { SessionId = id.SessionId };
            _workers.Add(id.SessionId, w);
            _workersVersion++;
            _workersCache = null;
            _workersCacheMs = 0;
        }
        w.Name = id.Name;
        w.UserAgent = id.UserAgent;
        w.Peer = id.Peer;
        w.Extranonce1 = id.Extranonce1;
        w.LastSeenMs = nowMs;
        return w;
    }

    public void RecordLateShare() => Interlocked.Increment(ref SharesLate);
    public void RecordStaleShare() => Interlocked.Increment(ref SharesStale);
    public void RecordUnknownJobShare() => Interlocked.Increment(ref SharesUnknownJob);
    public void RecordCleanBroadcast() => Interlocked.Increment(ref CleanBroadcasts);
    public void RecordCleanBroadcastClientTimeouts(int count)
    {
        if (count > 0)
            Interlocked.Add(ref CleanBroadcastClientTimeouts, count);
    }

    /// <summary>CPU-side parsing, merkle and header hashing time for a submitted share.</summary>
    public void RecordShareValidation(long elapsedStopwatchTicks) => RecordLatency(
        ref _shareValidationSamples,
        ref _shareValidationTotalTicks,
        ref _shareValidationMaxTicks,
        elapsedStopwatchTicks);

    /// <summary>mining.submit receipt until an accepted response enters the per-miner writer.</summary>
    public void RecordAcceptedShareAckQueued(long elapsedStopwatchTicks) => RecordLatency(
        ref _acceptedAckQueued,
        ref _acceptedAckQueueTotalTicks,
        ref _acceptedAckQueueMaxTicks,
        elapsedStopwatchTicks);

    /// <summary>mining.submit receipt until NetworkStream.WriteAsync completes.</summary>
    public void RecordAcceptedShareAckWritten(long elapsedStopwatchTicks) => RecordLatency(
        ref _acceptedAckWritten,
        ref _acceptedAckWriteTotalTicks,
        ref _acceptedAckWriteMaxTicks,
        elapsedStopwatchTicks);

    public long ShareValidationSamples => Interlocked.Read(ref _shareValidationSamples);
    public double ShareValidationAverageMs => AverageMilliseconds(
        ref _shareValidationTotalTicks, ref _shareValidationSamples);
    public double ShareValidationMaxMs => ToMilliseconds(Interlocked.Read(ref _shareValidationMaxTicks));
    public long AcceptedShareAckQueued => Interlocked.Read(ref _acceptedAckQueued);
    public long AcceptedShareAckWritten => Interlocked.Read(ref _acceptedAckWritten);
    public double AcceptedShareAckQueueAverageMs => AverageMilliseconds(
        ref _acceptedAckQueueTotalTicks, ref _acceptedAckQueued);
    public double AcceptedShareAckQueueMaxMs => ToMilliseconds(Interlocked.Read(ref _acceptedAckQueueMaxTicks));
    public double AcceptedShareAckWriteAverageMs => AverageMilliseconds(
        ref _acceptedAckWriteTotalTicks, ref _acceptedAckWritten);
    public double AcceptedShareAckWriteMaxMs => ToMilliseconds(Interlocked.Read(ref _acceptedAckWriteMaxTicks));

    private static void RecordLatency(
        ref long samples,
        ref long totalTicks,
        ref long maxTicks,
        long elapsedStopwatchTicks)
    {
        var elapsed = Math.Max(0, elapsedStopwatchTicks);
        Interlocked.Increment(ref samples);
        Interlocked.Add(ref totalTicks, elapsed);
        var observed = Interlocked.Read(ref maxTicks);
        while (elapsed > observed)
        {
            var previous = Interlocked.CompareExchange(ref maxTicks, elapsed, observed);
            if (previous == observed)
                break;
            observed = previous;
        }
    }

    private static double AverageMilliseconds(ref long totalTicks, ref long samples)
    {
        var count = Interlocked.Read(ref samples);
        return count == 0
            ? 0
            : ToMilliseconds(Interlocked.Read(ref totalTicks)) / count;
    }

    private static double ToMilliseconds(long stopwatchTicks) =>
        stopwatchTicks * 1000.0 / Stopwatch.Frequency;

    private long UtcNowMilliseconds() =>
        _utcNowMilliseconds?.Invoke() ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>
    /// H/s = sum(difficulty) * 2^32 / effective window. The full window is used after
    /// warm-up; before that, the denominator is the observed span with a small floor.
    /// </summary>
    private static double ComputeHashrateHps(double sumDiff, long oldestTimestampMs, long nowMs)
    {
        if (sumDiff <= 0)
            return 0;

        var spanMs = nowMs - oldestTimestampMs;
        double windowSec;
        if (spanMs >= HashrateWindowMs * 9 / 10)
            windowSec = HashrateWindowMs / 1000.0;
        else
            windowSec = Math.Max(HashrateMinWindowMs, spanMs) / 1000.0;

        if (windowSec < 1e-6)
            return 0;

        return sumDiff * HashesPerDifficulty / windowSec;
    }

    private static void AddHashrateSample(HashrateBucket[] buckets, long nowMs, double difficulty)
    {
        var bucketStart = nowMs - nowMs % HashrateBucketMs;
        var index = (int)((bucketStart / HashrateBucketMs) % buckets.Length);
        ref var bucket = ref buckets[index];
        if (bucket.StartTimestampMs != bucketStart)
        {
            bucket.StartTimestampMs = bucketStart;
            bucket.OldestTimestampMs = nowMs;
            bucket.SumDifficulty = difficulty;
            return;
        }
        bucket.SumDifficulty += difficulty;
        if (bucket.OldestTimestampMs == 0 || nowMs < bucket.OldestTimestampMs)
            bucket.OldestTimestampMs = nowMs;
    }

    private static HashrateAggregate AggregateHashrateBuckets(HashrateBucket[] buckets, long nowMs)
    {
        var cutoff = nowMs - HashrateWindowMs;
        var sum = 0d;
        var oldest = long.MaxValue;
        for (var i = 0; i < buckets.Length; i++)
        {
            ref readonly var bucket = ref buckets[i];
            // A 5-second bucket remains live while any part overlaps the exact window.
            // The extra ring slot prevents the current bucket from overwriting that
            // partially live boundary bucket.
            if (bucket.StartTimestampMs + HashrateBucketMs <= cutoff ||
                bucket.StartTimestampMs > nowMs ||
                bucket.SumDifficulty <= 0)
                continue;
            sum += bucket.SumDifficulty;
            oldest = Math.Min(oldest, bucket.OldestTimestampMs);
        }
        return new HashrateAggregate(sum, oldest == long.MaxValue ? nowMs : oldest);
    }

    public struct HashrateBucket
    {
        public long StartTimestampMs;
        public long OldestTimestampMs;
        public double SumDifficulty;
    }

    private readonly record struct HashrateAggregate(double SumDifficulty, long OldestTimestampMs);
    private readonly record struct ShareSnapshot(
        string UserAgent,
        string Extranonce1,
        string Hash,
        double Difficulty,
        double ActualDiff,
        long TimestampMs);
    private readonly record struct WorkerSnapshot(
        string UserAgent,
        string Extranonce1,
        double BestDiff,
        double AssignedDifficulty,
        long LastShareMs,
        double SumDifficulty,
        long OldestTimestampMs);
}

/// <summary>Per-connection identity for metrics (session-scoped, internal only).</summary>
public sealed class WorkerIdentity
{
    /// <summary>Stable unique key for this Stratum TCP session (not exposed on API).</summary>
    public string SessionId { get; init; } = "";

    /// <summary>mining.authorize username / payout address (internal only; not exposed on API).</summary>
    public string Name { get; init; } = "worker";

    public string UserAgent { get; init; } = "Unknown";
    /// <summary>Remote IP:port (internal only; not exposed on API).</summary>
    public string Peer { get; init; } = "";
    public string Extranonce1 { get; init; } = "";

    /// <summary>
    /// When true, <see cref="Normalize"/> returns this instance (session-cached identities).
    /// </summary>
    public bool IsNormalized { get; init; }

    public WorkerIdentity Normalize()
    {
        if (IsNormalized)
            return this;

        var sid = string.IsNullOrWhiteSpace(SessionId)
            ? Guid.NewGuid().ToString("N")
            : SessionId.Trim();
        return new WorkerIdentity
        {
            SessionId = sid,
            Name = string.IsNullOrWhiteSpace(Name) ? "worker" : Name.Trim(),
            UserAgent = string.IsNullOrWhiteSpace(UserAgent) ? "Unknown" : UserAgent.Trim(),
            Peer = Peer?.Trim() ?? "",
            Extranonce1 = Extranonce1?.Trim() ?? "",
            IsNormalized = true
        };
    }
}

public sealed class WorkerStats
{
    public string SessionId { get; set; } = "";
    public string Name { get; set; } = "worker";
    public string UserAgent { get; set; } = "Unknown";
    public string Peer { get; set; } = "";
    public string Extranonce1 { get; set; } = "";
    public double BestDiff { get; set; }
    public double AssignedDifficulty { get; set; }
    public long LastSeenMs { get; set; }
    public long LastShareMs { get; set; }
    public MetricsStore.HashrateBucket[] HashrateBuckets { get; } =
        new MetricsStore.HashrateBucket[MetricsStore.HashrateBucketCount];
}

/// <summary>In-memory share event. Sensitive fields stay internal; API maps to ShareDto.</summary>
public sealed class ShareEvent
{
    public string SessionId { get; set; } = "";
    public string Worker { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public string Peer { get; set; } = "";
    public string Extranonce1 { get; set; } = "";
    public double Difficulty { get; set; }
    public double ActualDiff { get; set; }
    public long TimestampMs { get; set; }
}

public sealed class BlockEvent
{
    public uint Height { get; set; }
    public string Hash { get; set; } = "";
    public string Result { get; set; } = "";
    public long TimestampMs { get; set; }
}

/// <summary>Public worker row: identify miners by extranonce1 + user-agent only.</summary>
public sealed class WorkerDto
{
    public string user_agent { get; set; } = "";
    public string extranonce1 { get; set; } = "";
    public double best_diff { get; set; }
    /// <summary>Legacy: TH/s (hashrate_hps / 1e12).</summary>
    public double hashrate { get; set; }
    /// <summary>Measured hashrate in H/s (sliding window).</summary>
    public double hashrate_hps { get; set; }
    public double assigned_difficulty { get; set; }
    public long last_share_ms { get; set; }
}

/// <summary>Public share row: share hash plus extranonce1 and user-agent identity.</summary>
public sealed class ShareDto
{
    public string user_agent { get; set; } = "";
    public string extranonce1 { get; set; } = "";
    public string hash { get; set; } = "";
    public double difficulty { get; set; }
    public double actual_diff { get; set; }
    public long timestamp_ms { get; set; }
}

public sealed class BlockDto
{
    public uint height { get; set; }
    public string hash { get; set; } = "";
    public string result { get; set; } = "";
    public long timestamp_ms { get; set; }
}
