using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Submit;

/// <summary>
/// Solo block submission (submit-first, persist-on-failure):
/// 1) Enqueue in memory and submitblock ASAP (no disk on the happy path)
/// 2) Single consumer with retries
/// 3) Inconclusive results persist immediately; transport failures persist after retries
/// 4) Consensus rejects → archive to failed-blocks/
/// 5) Startup re-queues any pending files from prior failures
/// </summary>
public sealed class BlockSubmitQueue
{
    private static readonly int[] DefaultAttemptDelaysMs =
        [0, 200, 500, 1000, 2000, 4000, 8000, 15_000, 30_000];

    private readonly BitcoinRpcClient _rpc;
    private readonly MetricsStore _metrics;
    private readonly string _pendingDir;
    private readonly string _failedDir;
    private readonly int[] _attemptDelaysMs;
    private readonly int _delayedRetryDelayMs;
    private readonly Channel<PendingBlock> _channel = Channel.CreateUnbounded<PendingBlock>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    private readonly object _chainStateLock = new();
    private TaskCompletionSource _chainStateChanged = NewChainStateSignal();
    private long _chainStateVersion;
    private Func<CancellationToken, Task>? _onAccepted;
    private readonly object _lifecycleLock = new();
    private readonly HashSet<PendingBlock> _delayedRetries = new();
    private readonly HashSet<Task> _delayedRetryTasks = new();
    private readonly CancellationTokenSource _delayedRetryCts = new();
    private Task? _runTask;
    private CancellationTokenSource? _runCts;
    private Task? _stopTask;
    private int _started;
    private int _stopping;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public BlockSubmitQueue(AppConfig cfg, BitcoinRpcClient rpc, MetricsStore metrics)
        : this(cfg, rpc, metrics, DefaultAttemptDelaysMs, delayedRetryDelayMs: 60_000)
    {
    }

    internal BlockSubmitQueue(
        AppConfig cfg,
        BitcoinRpcClient rpc,
        MetricsStore metrics,
        IReadOnlyList<int> attemptDelaysMs,
        int delayedRetryDelayMs)
    {
        ArgumentNullException.ThrowIfNull(attemptDelaysMs);
        if (attemptDelaysMs.Count == 0 || attemptDelaysMs.Any(delay => delay < 0))
            throw new ArgumentException("at least one non-negative attempt delay is required", nameof(attemptDelaysMs));
        if (delayedRetryDelayMs < 0)
            throw new ArgumentOutOfRangeException(nameof(delayedRetryDelayMs));

        _rpc = rpc;
        _metrics = metrics;
        _attemptDelaysMs = attemptDelaysMs.ToArray();
        _delayedRetryDelayMs = delayedRetryDelayMs;
        var root = ResolveDataDir(cfg.Runtime.DataDir);
        _pendingDir = Path.Combine(root, "pending-blocks");
        _failedDir = Path.Combine(root, "failed-blocks");
        Directory.CreateDirectory(_pendingDir);
        Directory.CreateDirectory(_failedDir);
    }

    public string PendingDir => _pendingDir;
    internal bool IsStopped => _stopTask?.IsCompletedSuccessfully == true;
    internal (int PendingCount, int TaskCount) DelayedRetryState
    {
        get
        {
            lock (_lifecycleLock)
                return (_delayedRetries.Count, _delayedRetryTasks.Count);
        }
    }

    /// <summary>Optional hook after submitblock accepted (e.g. refresh GBT).</summary>
    public void SetOnAccepted(Func<CancellationToken, Task> onAccepted) => _onAccepted = onAccepted;

    /// <summary>
    /// Wake a block waiting for its P2P-announced parent after Core publishes an
    /// authoritative template on the new chain tip.
    /// </summary>
    public void NotifyChainStateChanged()
    {
        TaskCompletionSource signal;
        lock (_chainStateLock)
        {
            signal = _chainStateChanged;
            _chainStateChanged = NewChainStateSignal();
            _chainStateVersion++;
        }
        signal.TrySetResult();
    }

    public Task StartAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        lock (_lifecycleLock)
        {
            if (Volatile.Read(ref _stopping) != 0)
                throw new InvalidOperationException("block submit queue is stopping");
            if (Volatile.Read(ref _started) != 0)
                return Task.CompletedTask;

            RecoverPendingIntoChannel();
            _runCts = new CancellationTokenSource();
            _runTask = Task.Run(() => RunLoopAsync(_runCts.Token), CancellationToken.None);
            Volatile.Write(ref _started, 1);
        }
        SoloLog.Info("block submit queue ready",
            ("mode", "submit_first_persist_on_fail"),
            ("pending_dir", _pendingDir),
            ("failed_dir", _failedDir));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stop accepting new blocks, drain queued submissions, and only then return.
    /// Normal operation remains submit-first; the timeout is a final shutdown escape
    /// hatch that persists the in-flight and queued blocks before process exit.
    /// </summary>
    public Task StopAsync(TimeSpan? drainTimeout = null)
    {
        lock (_lifecycleLock)
        {
            _stopTask ??= StopCoreAsync(drainTimeout ?? TimeSpan.FromMinutes(2));
            return _stopTask;
        }
    }

    /// <summary>
    /// Memory enqueue only (no disk). Returns immediately after the work is queued.
    /// RPC submit runs on the background consumer; disk persist only if submit still fails.
    /// </summary>
    public Task EnqueueFoundBlockAsync(string blockHex, string blockHashHex, uint height)
    {
        if (!IsValidBlockHex(blockHex, blockHashHex))
            throw new ArgumentException("block hex/hash is malformed or does not match the header");

        var hash = blockHashHex.ToLowerInvariant();
        // Block hex from ShareValidator/Hex.Encode is already lowercase; avoid a full-string copy.
        var pending = new PendingBlock
        {
            Height = height,
            Hash = hash,
            BlockHex = blockHex,
            CreatedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            FromDisk = false
        };

        SoloLog.Alert("block queued for submit (memory; no disk yet)",
            ("height", height),
            ("hash", hash));

        lock (_lifecycleLock)
        {
            if (Volatile.Read(ref _stopping) != 0)
                throw new InvalidOperationException("block submit queue is stopping");
            if (!_channel.Writer.TryWrite(pending))
                throw new InvalidOperationException("block submit queue is unavailable");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Binary happy path. Hex is generated only if the candidate must be persisted or archived.
    /// </summary>
    public Task EnqueueFoundBlockAsync(BlockCandidate block, string blockHashHex, uint height)
    {
        ArgumentNullException.ThrowIfNull(block);
        if (!IsValidBlockBytes(block.Bytes.Span, blockHashHex))
            throw new ArgumentException(
                "block hash is malformed or does not match the header", nameof(blockHashHex));

        var hash = blockHashHex.ToLowerInvariant();
        var pending = new PendingBlock
        {
            Height = height,
            Hash = hash,
            Candidate = block,
            CreatedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            FromDisk = false
        };

        SoloLog.Alert("block queued for submit (binary memory; no disk yet)",
            ("height", height),
            ("hash", hash));

        lock (_lifecycleLock)
        {
            if (Volatile.Read(ref _stopping) != 0)
                throw new InvalidOperationException("block submit queue is stopping");
            if (!_channel.Writer.TryWrite(pending))
                throw new InvalidOperationException("block submit queue is unavailable");
        }

        return Task.CompletedTask;
    }

    private void RecoverPendingIntoChannel()
    {
        string[] files;
        string[] tempFiles;
        try
        {
            files = Directory.GetFiles(_pendingDir, "*.json");
            tempFiles = Directory.GetFiles(_pendingDir, "*.json.tmp");
        }
        catch (Exception ex)
        {
            SoloLog.Alert("failed to scan pending blocks", ("dir", _pendingDir), ("error", ex.Message));
            return;
        }

        if (files.Length == 0 && tempFiles.Length == 0)
            return;

        var candidatePaths = new HashSet<string>(files, StringComparer.OrdinalIgnoreCase);
        foreach (var tempFile in tempFiles)
            candidatePaths.Add(tempFile[..^4]);
        files = candidatePaths.ToArray();
        Array.Sort(files, StringComparer.Ordinal);
        SoloLog.Alert("recovering pending blocks from disk", ("count", files.Length), ("dir", _pendingDir));

        foreach (var file in files)
        {
            var tempFile = file + ".tmp";
            PendingBlock? pending;
            if (File.Exists(file) && TryLoadPending(file, out pending))
            {
                DeleteStaleTemp(tempFile);
            }
            else
            {
                if (!File.Exists(tempFile) || !TryLoadPending(tempFile, out pending))
                    continue;

                try
                {
                    File.Move(tempFile, file, overwrite: true);
                    SoloLog.Alert("recovered pending block temp file", ("path", file));
                }
                catch (Exception ex)
                {
                    SoloLog.Alert("failed to promote pending block temp file",
                        ("path", tempFile), ("error", ex.Message));
                    continue;
                }
            }

            pending!.Hash = pending.Hash.ToLowerInvariant();
            pending.BlockHex = pending.BlockHex.ToLowerInvariant();
            pending.FromDisk = true;
            if (!_channel.Writer.TryWrite(pending))
                SoloLog.Alert("could not re-queue pending block", ("hash", pending.Hash));
            else
                SoloLog.Alert("re-queued pending block", ("height", pending.Height), ("hash", pending.Hash));
        }
    }

    private static bool TryLoadPending(string path, out PendingBlock? pending)
    {
        pending = null;
        try
        {
            var json = File.ReadAllText(path);
            pending = JsonSerializer.Deserialize<PendingBlock>(json, JsonOpts);
            var fileHash = PendingHashFromPath(path);
            if (pending != null && fileHash != null &&
                string.Equals(fileHash, pending.Hash, StringComparison.OrdinalIgnoreCase) &&
                IsValidBlockHex(pending.BlockHex, pending.Hash))
                return true;

            SoloLog.Alert("skip corrupt pending block file", ("path", path));
        }
        catch (Exception ex)
        {
            SoloLog.Alert("failed to load pending block", ("path", path), ("error", ex.Message));
        }
        pending = null;
        return false;
    }

    private static void DeleteStaleTemp(string tempFile)
    {
        if (!File.Exists(tempFile))
            return;
        try
        {
            File.Delete(tempFile);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("failed to remove stale pending block temp file",
                ("path", tempFile), ("error", ex.Message));
        }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var pending in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await SubmitOnceWithRetriesAsync(pending, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    // A bounded graceful shutdown may cancel an in-flight RPC. Preserve
                    // that block before leaving; queued items are drained by StopCoreAsync.
                    var persisted = false;
                    try
                    {
                        await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                        persisted = true;
                    }
                    catch (Exception ex)
                    {
                        SoloLog.Alert("failed to persist block during submit queue shutdown",
                            ("hash", pending.Hash),
                            ("height", pending.Height),
                            ("error", ex.Message));
                    }
                    if (!persisted)
                    {
                        ScheduleDelayedRetry(
                            pending, GetChainStateVersion(), _delayedRetryDelayMs);
                    }
                    break;
                }
                catch (Exception ex)
                {
                    // Best effort: ensure hex is on disk, then re-queue.
                    SoloLog.Alert("submit loop error; will persist and retry",
                        ("hash", pending.Hash),
                        ("height", pending.Height),
                        ("error", ex.Message));
                    try
                    {
                        await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception pex)
                    {
                        SoloLog.Alert("persist after loop error failed",
                            ("hash", pending.Hash),
                            ("error", pex.Message));
                    }

                    ScheduleDelayedRetry(pending, GetChainStateVersion(), delayMs: 5_000);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // shutdown
        }
    }

    private async Task SubmitOnceWithRetriesAsync(PendingBlock pending, CancellationToken ct)
    {
        // Fast first attempts for propagation; longer delays if RPC is unhealthy.
        Exception? last = null;
        var waitingForParent = pending.WaitingForParent;
        var parentWaitVersion = GetChainStateVersion();

        foreach (var delay in _attemptDelaysMs)
        {
            if (delay > 0)
            {
                if (waitingForParent)
                    await WaitForChainStateOrDelayAsync(delay, parentWaitVersion, ct).ConfigureAwait(false);
                else
                    await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            try
            {
                var chainStateBeforeSubmit = GetChainStateVersion();
                var result = pending.Candidate != null
                    ? await _rpc.SubmitBlockAsync(pending.Candidate, ct).ConfigureAwait(false)
                    : await _rpc.SubmitBlockAsync(pending.GetBlockHex(), ct).ConfigureAwait(false);

                if (result == null)
                {
                    DeletePending(pending.Hash);
                    _metrics.RecordBlock(pending.Height, pending.Hash, "submitted");
                    SoloLog.Info("submitblock accepted", ("height", pending.Height), ("hash", pending.Hash));
                    await InvokeOnAcceptedAsync(ct).ConfigureAwait(false);
                    return;
                }

                if (string.Equals(result, "duplicate", StringComparison.OrdinalIgnoreCase))
                {
                    DeletePending(pending.Hash);
                    _metrics.RecordBlock(pending.Height, pending.Hash, "duplicate");
                    SoloLog.Info("submitblock duplicate", ("height", pending.Height), ("hash", pending.Hash));
                    return;
                }

                if (IsMissingParentResult(result))
                {
                    waitingForParent = true;
                    pending.WaitingForParent = true;
                    parentWaitVersion = chainStateBeforeSubmit;
                    last = new InvalidOperationException($"submitblock deferred: {result}");
                    SoloLog.Warn("submitblock waiting for parent block",
                        ("height", pending.Height),
                        ("hash", pending.Hash),
                        ("reason", result));
                    continue;
                }

                if (IsInconclusiveResult(result))
                {
                    last = new InvalidOperationException($"submitblock inconclusive: {result}");

                    // Core has not made a conclusive validity decision. Keep the full
                    // candidate durable before retrying; it must never enter failed-blocks.
                    await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                    if (!pending.CountedInconclusive)
                    {
                        _metrics.RecordBlock(pending.Height, pending.Hash, "inconclusive");
                        pending.CountedInconclusive = true;
                    }
                    SoloLog.Warn("submitblock inconclusive; persisted and will retry",
                        ("height", pending.Height),
                        ("hash", pending.Hash),
                        ("reason", result));
                    continue;
                }

                // Consensus / policy rejection — do not spin forever; archive for forensics.
                _metrics.RecordBlock(pending.Height, pending.Hash, "rejected");
                SoloLog.Alert("submitblock rejected (archived)",
                    ("height", pending.Height),
                    ("hash", pending.Hash),
                    ("reason", result));
                ArchiveFailed(pending, result);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                last = ex;
                SoloLog.Warn("submitblock attempt failed",
                    ("hash", pending.Hash),
                    ("height", pending.Height),
                    ("error", ex.Message));
            }
        }

        // Transport / node still failing — only now pay for disk I/O so a crash can recover.
        // Record metrics once per "wave" (first failure only) so re-queues do not inflate counters.
        if (!pending.CountedSubmitFailed)
        {
            _metrics.RecordBlock(pending.Height, pending.Hash, "submit_failed");
            pending.CountedSubmitFailed = true;
        }

        try
        {
            await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
            SoloLog.Alert("submitblock exhausted retries; persisted for recovery",
                ("height", pending.Height),
                ("hash", pending.Hash),
                ("path", PathForPending(pending.Hash)),
                ("error", last?.Message ?? "unknown"));
        }
        catch (Exception ex)
        {
            SoloLog.Alert("submitblock exhausted retries AND persist failed — block only in memory until re-queue",
                ("height", pending.Height),
                ("hash", pending.Hash),
                ("submit_error", last?.Message ?? "unknown"),
                ("persist_error", ex.Message));
        }

        // Transfer ownership to the tracked delayed-retry set. If shutdown has started,
        // StopCoreAsync will persist this candidate after the run loop exits.
        ScheduleDelayedRetry(pending, parentWaitVersion, _delayedRetryDelayMs);
    }

    private void ScheduleDelayedRetry(PendingBlock pending, long parentWaitVersion, int delayMs)
    {
        lock (_lifecycleLock)
        {
            _delayedRetries.Add(pending);
            if (Volatile.Read(ref _stopping) != 0)
                return;

            Task? retryTask = null;
            retryTask = Task.Run(async () =>
            {
                try
                {
                    if (pending.WaitingForParent)
                    {
                        await WaitForChainStateOrDelayAsync(
                            delayMs,
                            parentWaitVersion,
                            _delayedRetryCts.Token).ConfigureAwait(false);
                    }
                    else
                    {
                        await Task.Delay(delayMs, _delayedRetryCts.Token).ConfigureAwait(false);
                    }

                    lock (_lifecycleLock)
                    {
                        if (Volatile.Read(ref _stopping) != 0)
                            return;
                        if (!_channel.Writer.TryWrite(pending))
                            throw new InvalidOperationException("block submit queue rejected a delayed retry");
                        // The channel now owns this candidate. Removing it under the same
                        // lock used by StopAsync prevents duplicate shutdown persistence.
                        _delayedRetries.Remove(pending);
                    }
                }
                catch (OperationCanceledException) when (_delayedRetryCts.IsCancellationRequested)
                {
                    // StopCoreAsync owns the tracked candidate and persists it before returning.
                }
                catch (Exception ex)
                {
                    SoloLog.Alert("delayed submitblock retry failed",
                        ("hash", pending.Hash),
                        ("height", pending.Height),
                        ("error", ex.Message));
                }
                finally
                {
                    lock (_lifecycleLock)
                        _delayedRetryTasks.Remove(retryTask!);
                }
            }, CancellationToken.None);
            _delayedRetryTasks.Add(retryTask);
        }
    }

    private async Task WaitForChainStateOrDelayAsync(
        int delayMs, long observedVersion, CancellationToken ct)
    {
        Task signalTask;
        lock (_chainStateLock)
        {
            if (_chainStateVersion != observedVersion)
                return;
            signalTask = _chainStateChanged.Task;
        }

        try
        {
            await signalTask
                .WaitAsync(TimeSpan.FromMilliseconds(delayMs), ct)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Periodic retry remains the fallback if no authoritative GBT arrives.
        }
    }

    private static TaskCompletionSource NewChainStateSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private long GetChainStateVersion()
    {
        lock (_chainStateLock)
            return _chainStateVersion;
    }

    internal static bool IsMissingParentResult(string? result) =>
        string.Equals(result, "prev-blk-not-found", StringComparison.OrdinalIgnoreCase);

    internal static bool IsInconclusiveResult(string? result) =>
        string.Equals(result, "inconclusive", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(result, "duplicate-inconclusive", StringComparison.OrdinalIgnoreCase);

    private async Task StopCoreAsync(TimeSpan drainTimeout)
    {
        if (drainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(drainTimeout));

        Interlocked.Exchange(ref _stopping, 1);
        _channel.Writer.TryComplete();
        _delayedRetryCts.Cancel();

        var runTask = _runTask;
        if (runTask != null)
        {
            using var drainDelayCts = new CancellationTokenSource();
            var drainDelay = Task.Delay(drainTimeout, drainDelayCts.Token);
            var completed = await Task.WhenAny(runTask, drainDelay).ConfigureAwait(false);
            if (completed != runTask)
            {
                SoloLog.Warn("submit queue drain timeout; persisting remaining blocks",
                    ("timeout_secs", drainTimeout.TotalSeconds));
                _runCts?.Cancel();
            }
            else
            {
                drainDelayCts.Cancel();
            }

            try
            {
                await runTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SoloLog.Alert("submit queue stopped with an error",
                    ("error", ex.Message));
            }
        }

        // No new delayed task can be scheduled after the single run loop has exited.
        // Await cancellation completion before disposing its CTS or snapshotting owners.
        Task[] delayedTasks;
        lock (_lifecycleLock)
            delayedTasks = _delayedRetryTasks.ToArray();
        if (delayedTasks.Length != 0)
        {
            try
            {
                await Task.WhenAll(delayedTasks).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SoloLog.Alert("delayed submitblock tasks stopped with an error",
                    ("error", ex.Message));
            }
        }

        while (_channel.Reader.TryRead(out var pending))
        {
            try
            {
                await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SoloLog.Alert("failed to persist queued block during shutdown",
                    ("hash", pending.Hash),
                    ("height", pending.Height),
                    ("error", ex.Message));
            }
        }

        PendingBlock[] delayedToPersist;
        lock (_lifecycleLock)
            delayedToPersist = _delayedRetries.ToArray();
        foreach (var pending in delayedToPersist)
        {
            try
            {
                await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                lock (_lifecycleLock)
                    _delayedRetries.Remove(pending);
            }
            catch (Exception ex)
            {
                SoloLog.Alert("failed to persist delayed block during shutdown",
                    ("hash", pending.Hash),
                    ("height", pending.Height),
                    ("error", ex.Message));
            }
        }

        _runCts?.Dispose();
        _runCts = null;
        _delayedRetryCts.Dispose();
    }

    private async Task PersistPendingAsync(PendingBlock pending, CancellationToken ct)
    {
        var path = PathForPending(pending.Hash);
        if (File.Exists(path))
        {
            pending.FromDisk = true;
            return;
        }

        await WritePendingAtomicAsync(path, pending, ct).ConfigureAwait(false);
        pending.FromDisk = true;
        SoloLog.Alert("block persisted for submit recovery",
            ("height", pending.Height),
            ("hash", pending.Hash),
            ("path", path));
    }

    private async Task InvokeOnAcceptedAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _stopping) != 0)
            return;
        var hook = _onAccepted;
        if (hook == null)
            return;
        try
        {
            await hook(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("post-submit hook failed", ("error", ex.Message));
        }
    }

    private string PathForPending(string hash)
    {
        if (!IsHash256Hex(hash))
            throw new ArgumentException("block hash must be exactly 32 bytes of hex", nameof(hash));
        return Path.Combine(_pendingDir, $"{hash.ToLowerInvariant()}.json");
    }

    private static string? PendingHashFromPath(string path)
    {
        var fileName = Path.GetFileName(path);
        const string finalSuffix = ".json";
        const string tempSuffix = ".json.tmp";
        string hash;
        if (fileName.EndsWith(tempSuffix, StringComparison.OrdinalIgnoreCase))
            hash = fileName[..^tempSuffix.Length];
        else if (fileName.EndsWith(finalSuffix, StringComparison.OrdinalIgnoreCase))
            hash = fileName[..^finalSuffix.Length];
        else
            return null;

        // Files created by this process are canonical lowercase. Reject aliases so
        // cleanup can never target a different path on a case-sensitive filesystem.
        return hash.Equals(hash.ToLowerInvariant(), StringComparison.Ordinal) &&
               IsHash256Hex(hash)
            ? hash
            : null;
    }

    private static bool IsValidBlockHex(string? blockHex, string? blockHashHex)
    {
        if (blockHex == null || blockHex.Length < 162 ||
            !BitcoinEncoding.IsExactHex(blockHex.AsSpan()) || !IsHash256Hex(blockHashHex))
            return false;

        Span<byte> header = stackalloc byte[80];
        return BitcoinEncoding.TryDecodeExactHex(blockHex.AsSpan(0, 160), header) &&
               HeaderMatchesHash(header, blockHashHex!);
    }

    private static bool IsValidBlockBytes(ReadOnlySpan<byte> blockBytes, string? blockHashHex) =>
        blockBytes.Length >= 81 && IsHash256Hex(blockHashHex) &&
        HeaderMatchesHash(blockBytes[..80], blockHashHex!);

    private static bool IsHash256Hex(string? hash)
    {
        if (hash == null || hash.Length != 64)
            return false;
        Span<byte> decoded = stackalloc byte[32];
        return BitcoinEncoding.TryDecodeExactHex(hash.AsSpan(), decoded);
    }

    private static bool HeaderMatchesHash(ReadOnlySpan<byte> header, string hashHex)
    {
        Span<byte> expectedBe = stackalloc byte[32];
        Span<byte> actualLe = stackalloc byte[32];
        if (!BitcoinEncoding.TryDecodeExactHex(hashHex.AsSpan(), expectedBe))
            return false;
        BitcoinEncoding.DoubleSha256(header, actualLe);
        for (var i = 0; i < 32; i++)
        {
            if (actualLe[i] != expectedBe[31 - i])
                return false;
        }
        return true;
    }

    private static async Task WritePendingAtomicAsync(string path, PendingBlock pending, CancellationToken ct)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);
        var tmp = path + ".tmp";
        // Do not serialize FromDisk (runtime-only).
        var wire = new PendingBlockWire
        {
            Height = pending.Height,
            Hash = pending.Hash,
            BlockHex = pending.GetBlockHex(),
            CreatedAtUnix = pending.CreatedAtUnix
        };
        var json = JsonSerializer.Serialize(wire, JsonOpts);
        await File.WriteAllTextAsync(tmp, json, ct).ConfigureAwait(false);

        try
        {
            await using var fs = new FileStream(tmp, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Flush(flushToDisk: true);
        }
        catch
        {
            // Some filesystems ignore Flush(true); file is still renamed below.
        }

        File.Move(tmp, path, overwrite: true);
    }

    private void DeletePending(string hash)
    {
        var path = PathForPending(hash);
        try
        {
            if (File.Exists(path))
                File.Delete(path);
            var tmp = path + ".tmp";
            if (File.Exists(tmp))
                File.Delete(tmp);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("failed to delete pending block file", ("path", path), ("error", ex.Message));
        }
    }

    private void ArchiveFailed(PendingBlock pending, string reason)
    {
        var pendingPath = PathForPending(pending.Hash);
        var failedPath = Path.Combine(_failedDir, $"{pending.Hash}.json");
        try
        {
            var archived = new FailedBlock
            {
                Height = pending.Height,
                Hash = pending.Hash,
                BlockHex = pending.GetBlockHex(),
                CreatedAtUnix = pending.CreatedAtUnix,
                FailedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Reason = reason
            };
            File.WriteAllText(failedPath, JsonSerializer.Serialize(archived, JsonOpts));
            if (File.Exists(pendingPath))
                File.Delete(pendingPath);
        }
        catch (Exception ex)
        {
            SoloLog.Alert("failed to archive rejected block",
                ("hash", pending.Hash),
                ("error", ex.Message),
                ("pending", pendingPath));
        }
    }

    private static string ResolveDataDir(string dataDir)
    {
        if (string.IsNullOrWhiteSpace(dataDir))
            dataDir = "data";
        if (Path.IsPathRooted(dataDir))
            return dataDir;
        return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), dataDir));
    }

    private sealed class PendingBlock
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        [JsonIgnore] public BlockCandidate? Candidate { get; set; }
        public long CreatedAtUnix { get; set; }
        [JsonIgnore] public bool FromDisk { get; set; }
        /// <summary>Runtime: avoid double-counting submit_failed on re-queue waves.</summary>
        [JsonIgnore] public bool CountedSubmitFailed { get; set; }
        /// <summary>Runtime: record an inconclusive result once while retries continue.</summary>
        [JsonIgnore] public bool CountedInconclusive { get; set; }
        /// <summary>Runtime: retry promptly when Core learns a P2P-announced parent.</summary>
        [JsonIgnore] public bool WaitingForParent { get; set; }

        public string GetBlockHex()
        {
            if (!string.IsNullOrEmpty(BlockHex))
                return BlockHex;
            return BlockHex = Candidate?.GetHex() ??
                throw new InvalidOperationException("pending block has no serialized payload");
        }
    }

    /// <summary>On-disk shape (no runtime flags).</summary>
    private sealed class PendingBlockWire
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        public long CreatedAtUnix { get; set; }
    }

    private sealed class FailedBlock
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        public long CreatedAtUnix { get; set; }
        public long FailedAtUnix { get; set; }
        public string Reason { get; set; } = "";
    }
}
