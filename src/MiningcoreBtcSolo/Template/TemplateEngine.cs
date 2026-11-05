using System.Threading.Channels;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Submit;
using MiningcoreBtcSolo.Util;
using NetMQ;
using NetMQ.Sockets;

namespace MiningcoreBtcSolo.Template;

/// <summary>
/// Solo template policy:
/// - P2P headers/cmpctblock → empty clean job only (no GBT on success path).
/// - ZMQ/direct GBT may overlap longpoll; their short apply phases remain serialized.
/// - Identical template key → skip, no stratum job notify.
/// - No rebroadcast / same-tip fee timer.
/// </summary>
public sealed class TemplateEngine
{
    public event Action<string>? P2pFastBlockPublished;

    private readonly AppConfig _cfg;
    private readonly BitcoinRpcClient _rpc;
    private readonly JobBuilder _builder;
    private readonly MetricsStore _metrics;
    private readonly BlockSubmitQueue _submitQueue;
    private readonly SemaphoreSlim _refreshSem = new(1, 1);
    private readonly SemaphoreSlim _gbtBuildSem = new(1, 1);
    private readonly object _directRefreshLock = new();
    private bool _directRefreshRunning;
    private bool _directRefreshPending;
    private TemplateSource _directCurrentSource;
    private TemplateSource _directPendingSource;
    private TaskCompletionSource? _directCurrentCompletion;
    private TaskCompletionSource? _directPendingCompletion;
    private CancellationToken _lifetimeToken;
    private bool _hasLifetimeToken;
    // Protects latest-state notification coalescing. It is never held while fan-out
    // or socket I/O runs, so P2P-fast/ZMQ/longpoll publication cannot be stalled by a miner.
    private readonly object _publicationLock = new();
    private readonly object _jobLock = new();
    private readonly List<JobRegistration> _jobs = new();
    private readonly Dictionary<ulong, JobRegistration> _jobsByKey = new();
    private readonly Dictionary<string, DateTimeOffset> _expiredJobIds = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, DateTimeOffset> _expiredJobKeys = new();
    private long _retainedTransactionBytes;
    private JobLookupSnapshot _jobLookupSnapshot = JobLookupSnapshot.Empty;
    private long _jobLookupSnapshotPublicationCount;
    // Active work may briefly be a P2P empty-fast job. Keep only dashboard and
    // identity metadata for the latest full GBT so reclaimed transaction sets
    // are not retained outside the configured job-memory budget.
    private JobTemplate _active = JobTemplate.Empty();
    private AuthoritativeJobSnapshot _authoritative = AuthoritativeJobSnapshot.Empty;
    private ChainTip? _tip;
    private string? _longpollId;
    private string? _lastTemplateKey;
    private long _lastZmqMs;
    private string? _lastAppliedGbtLongpollId;
    private GbtScalarIdentity? _lastAppliedGbtScalarIdentity;
    private long _appliedGbtGeneration;
    private long _jobEpoch;
    private readonly object _networkHashrateLock = new();
    private Task? _networkHashrateTask;
    private long _nextNetworkHashrateRefreshMs;
    // Latest-state notification: at most one JobTemplate (which may retain a full
    // block transaction set) waits for broadcast. Superseding updates merge the
    // clean flag so a required old-job invalidation can never be lost.
    private readonly Channel<bool> _notifySignal = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropWrite,
            AllowSynchronousContinuations = false
        });
    private JobNotify? _pendingNotify;

    public TemplateEngine(AppConfig cfg, BitcoinRpcClient rpc, MetricsStore metrics, BlockSubmitQueue submitQueue)
    {
        _cfg = cfg;
        _rpc = rpc;
        _metrics = metrics;
        _submitQueue = submitQueue;
        _builder = new JobBuilder(cfg);
        _submitQueue.SetOnAccepted(ct => RefreshDirectAsync(TemplateSource.PostSubmit, ct));
    }

    public async Task DispatchNotificationsAsync(Action<JobNotify> dispatch, CancellationToken ct)
    {
        while (await _notifySignal.Reader.WaitToReadAsync(ct))
        {
            while (_notifySignal.Reader.TryRead(out _)) { }
            JobNotify? pending;
            lock (_publicationLock)
            {
                pending = _pendingNotify;
                _pendingNotify = null;
            }

            if (pending.HasValue)
                dispatch(pending.Value);
        }
    }

    public AuthoritativeJobSnapshot AuthoritativeJob
    {
        get { lock (_jobLock) return _authoritative; }
    }

    public JobTemplate ActiveMiningJob
    {
        get { lock (_jobLock) return _active; }
    }

    public bool TryUseActiveMiningJob(Action<JobTemplate> action)
    {
        lock (_jobLock)
        {
            if (!_active.Ready || !_jobs.Any(x => ReferenceEquals(x.Job, _active)))
                return false;

            action(_active);
            return true;
        }
    }

    public bool TryUseAuthoritativeJob(Action<JobTemplate> action)
    {
        lock (_jobLock)
        {
            if (!_authoritative.Ready ||
                _active.Epoch != _authoritative.Epoch ||
                !_jobs.Any(x => ReferenceEquals(x.Job, _active)))
                return false;

            action(_active);
            return true;
        }
    }

    public JobLookupResult FindJob(string jobId)
    {
        lock (_jobLock)
        {
            JobRegistration? registration = null;
            for (var i = 0; i < _jobs.Count; i++)
            {
                var candidate = _jobs[i];
                if (!string.Equals(candidate.Job.JobId, jobId, StringComparison.Ordinal))
                    continue;
                registration = candidate;
                break;
            }
            if (registration != null)
            {
                return new JobLookupResult(
                    registration.RetiredByEpoch.HasValue
                        ? JobLookupStatus.RetiredWithinGrace
                        : JobLookupStatus.Available,
                    registration.Job);
            }

            var now = DateTimeOffset.UtcNow;
            if (_expiredJobIds.TryGetValue(jobId, out var tombstoneUntil))
            {
                if (tombstoneUntil > now)
                    return new JobLookupResult(JobLookupStatus.Expired, null);
                _expiredJobIds.Remove(jobId);
            }

            return new JobLookupResult(JobLookupStatus.Unknown, null);
        }
    }

    public JobLookupResult FindJob(ulong jobKey)
    {
        if (jobKey == 0)
            return new JobLookupResult(JobLookupStatus.Unknown, null);

        var snapshot = Volatile.Read(ref _jobLookupSnapshot);
        foreach (var entry in snapshot.Jobs)
        {
            if (entry.JobKey == jobKey)
                return new JobLookupResult(
                    entry.Retired
                        ? JobLookupStatus.RetiredWithinGrace
                        : JobLookupStatus.Available,
                    entry.Job);
        }

        var now = DateTimeOffset.UtcNow;
        foreach (var tombstone in snapshot.Expired)
        {
            if (tombstone.JobKey == jobKey && tombstone.ExpiresAt > now)
                return new JobLookupResult(JobLookupStatus.Expired, null);
        }

        return new JobLookupResult(JobLookupStatus.Unknown, null);
    }

    /// <summary>
    /// Marks every job retired by this or an earlier clean epoch as delivered to the
    /// live-client snapshot. Actual reclamation remains delayed for in-flight shares.
    /// A later epoch safely satisfies an earlier barrier after latest-wins coalescing.
    /// </summary>
    public void MarkCleanBroadcastComplete(long epoch, DateTimeOffset completedAt)
    {
        lock (_jobLock)
        {
            foreach (var entry in _jobs)
            {
                if (entry.RetiredByEpoch.HasValue && entry.RetiredByEpoch.Value <= epoch &&
                    !entry.BroadcastCompletedAt.HasValue)
                    entry.BroadcastCompletedAt = completedAt;
            }
            if (ReclaimJobsLocked(completedAt))
                PublishJobLookupSnapshotLocked();
        }
    }

    public void ReclaimRetiredJobs(DateTimeOffset now)
    {
        lock (_jobLock)
        {
            if (ReclaimJobsLocked(now))
                PublishJobLookupSnapshotLocked();
        }
    }

    internal long JobLookupSnapshotPublicationCount =>
        Interlocked.Read(ref _jobLookupSnapshotPublicationCount);

    public long MinimumRetainedEpoch
    {
        get
        {
            lock (_jobLock)
                return _jobs.Count == 0 ? Volatile.Read(ref _jobEpoch) : _jobs.Min(x => x.Job.Epoch);
        }
    }

    internal bool TryEnterPublicationForTest() => _refreshSem.Wait(0);

    internal void ExitPublicationForTest() => _refreshSem.Release();

    public async Task StartAsync(CancellationToken ct)
    {
        lock (_directRefreshLock)
        {
            _lifetimeToken = ct;
            _hasLifetimeToken = true;
        }
        await RefreshDirectAsync(TemplateSource.Startup, ct);
        _ = Task.Run(() => LongpollLoop(ct), ct);
        _ = Task.Run(() => StartupCatchupAsync(ct), ct);
        if (_cfg.Bitcoind.ZmqBlockUrls.Count > 0)
            _ = Task.Run(() => ZmqLoop(ct), ct);
    }

    /// <summary>
    /// Submit-first: memory-enqueue for immediate submitblock. Disk persist only if
    /// RPC still fails after retries (crash recovery via runtime.data_dir/pending-blocks).
    /// </summary>
    public Task SubmitBlockAsync(string blockHex, string blockHashHex, uint height)
        => _submitQueue.EnqueueFoundBlockAsync(blockHex, blockHashHex, height);

    public Task SubmitBlockAsync(BlockCandidate block, string blockHashHex, uint height)
        => _submitQueue.EnqueueFoundBlockAsync(block, blockHashHex, height);

    /// <summary>
    /// Solo: early empty clean job from P2P headers/cmpctblock only.
    /// Does not pull full GBT — ZMQ / longpoll win the full-template race.
    /// </summary>
    public Task HandleP2pFastAnnouncementAsync(
        string prevhashHex,
        string blockHashHex,
        uint blockTime,
        uint blockHeight,
        uint blockNbits,
        uint blockVersion,
        CancellationToken ct,
        string? peer = null)
        => TryPublishEmptyFastAsync(
            prevhashHex, blockHashHex, blockTime, blockHeight, blockNbits, blockVersion, ct, peer);

    private async Task TryPublishEmptyFastAsync(
        string prevhashHex,
        string blockHashHex,
        uint blockTime,
        uint blockHeight,
        uint blockNbits,
        uint blockVersion,
        CancellationToken ct,
        string? peer)
    {
        const TemplateSource source = TemplateSource.P2pFast;

        // Never discard a valid fast-path candidate merely because a full template is
        // in its short synchronous apply phase. Direct/longpoll RPC I/O does not hold
        // this semaphore; after the current apply completes we re-read and revalidate
        // the tip, then either publish the fast child or reject it as superseded.
        await _refreshSem.WaitAsync(ct);

        try
        {
            ChainTip? tip;
            JobTemplate current;
            lock (_jobLock)
            {
                tip = _tip;
                current = _active;
            }
            if (tip == null)
            {
                SoloLog.Debug("skip_job", ("reason", "no_tip"), ("source", SoloLog.SourceName(source)));
                return;
            }

            var rejection = ValidateP2pFastParentHeader(
                _cfg.NetworkName,
                tip,
                prevhashHex,
                blockHashHex,
                blockTime,
                blockHeight,
                blockNbits,
                blockVersion,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            if (rejection != null)
            {
                SoloLog.Debug("skip_job",
                    ("reason", rejection),
                    ("source", SoloLog.SourceName(source)),
                    ("height", blockHeight),
                    ("hash", blockHashHex));
                return;
            }

            var nextHeight = blockHeight + 1;
            if (!ShouldPublishFastEmpty(nextHeight))
            {
                SoloLog.Debug("skip_job",
                    ("reason", "empty_fast_guards"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", nextHeight),
                    ("hash", blockHashHex));
                return;
            }

            // Already advanced tip to this announcement — no re-notify.
            if (current.Ready &&
                string.Equals(current.PrevhashBe, blockHashHex, StringComparison.OrdinalIgnoreCase) &&
                current.Height == nextHeight)
            {
                SoloLog.Debug("skip_job",
                    ("reason", "already_on_tip"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", nextHeight),
                    ("hash", blockHashHex));
                return;
            }

            // Replacing one member of an 11-block median window with a timestamp
            // above its old median cannot move the new median above max(old MTP,
            // parent time). This conservative upper bound may skip valid fast work,
            // but it cannot produce a child whose nTime is too old.
            var conservativeMtpUpperBound = ConservativeMtpUpperBound(
                tip.MedianTimePast, blockTime);
            var job = _builder.BuildEmptyFast(
                tip, blockHashHex, nextHeight, conservativeMtpUpperBound, source);
            if (string.Equals(_lastTemplateKey, job.TemplateKey, StringComparison.Ordinal))
            {
                SoloLog.Debug("skip_job",
                    ("reason", "identical_template"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", job.Height));
                return;
            }

            PublishFastJob(job);
            lock (_jobLock)
            {
                _tip = new ChainTip
                {
                    HashHex = blockHashHex.ToLowerInvariant(),
                    Height = blockHeight,
                    MedianTimePast = conservativeMtpUpperBound,
                    Nbits = tip.Nbits,
                    Version = tip.Version,
                    Vbrequired = tip.Vbrequired,
                    TargetLe = tip.TargetLe.ToArray(),
                    NetworkDifficulty = tip.NetworkDifficulty
                };
            }
            SoloLog.Info(SoloLog.FormatTemplateEvent(
                "new block", source, job.JobId, job.Height, 0, job.CoinbaseValue, job.Nbits,
                cleanJobs: true, peer: peer, hash: blockHashHex));
            if (!string.IsNullOrWhiteSpace(peer))
                P2pFastBlockPublished?.Invoke(peer);
            // Solo: do not schedule GBT here — ZMQ / longpoll own full-template refresh.
        }
        finally
        {
            _refreshSem.Release();
        }
    }

    private bool ShouldPublishFastEmpty(uint nextHeight)
    {
        uint vbrequired;
        lock (_jobLock)
            vbrequired = _active.Ready ? _active.Vbrequired : 0;
        return AllowsP2pFastEmpty(_cfg.NetworkName, nextHeight, vbrequired);
    }

    internal static bool AllowsP2pFastEmpty(
        string networkName,
        uint nextHeight,
        uint vbrequired) =>
        (networkName.Equals("mainnet", StringComparison.OrdinalIgnoreCase) ||
         networkName.Equals("bitcoin", StringComparison.OrdinalIgnoreCase)) &&
        nextHeight % 2016 != 0 &&
        vbrequired == 0;

    private async Task LongpollLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var appliedGeneration = GetAppliedGbtGeneration();
            try
            {
                var param = BuildGbtParams(_longpollId);
                var gbt = await _rpc.CallLongpollAsync<GbtResponse>("getblocktemplate", param, ct);

                await ApplyGbtAsync(gbt, TemplateSource.Longpoll, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                ForgetAppliedGbtRevision(appliedGeneration);
                MarkRefreshFailed();
                SoloLog.Warn("longpoll error; falling back to direct GBT", ("error", ex.Message));
                try
                {
                    await RefreshDirectAsync(TemplateSource.LongpollFallback, ct);
                }
                catch (Exception ex2)
                {
                    SoloLog.Warn("direct GBT fallback failed", ("error", ex2.Message));
                    await Task.Delay(3000, ct);
                }
            }
        }
    }

    private async Task StartupCatchupAsync(CancellationToken ct)
    {
        try
        {
            // Core may serve a cached template for roughly five seconds after a
            // mempool update. One bounded catch-up prevents an empty startup job
            // from remaining authoritative until the longpoll mempool timeout.
            await Task.Delay(TimeSpan.FromSeconds(6), ct).ConfigureAwait(false);
            await RefreshDirectAsync(TemplateSource.Startup, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            SoloLog.Warn("startup template catch-up failed", ("error", ex.Message));
        }
    }

    private void ZmqLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var sub = new SubscriberSocket();
                sub.Options.Linger = TimeSpan.Zero;
                foreach (var url in _cfg.Bitcoind.ZmqBlockUrls.Where(u => !string.IsNullOrWhiteSpace(u)))
                {
                    sub.Connect(url);
                    SoloLog.Info("ZMQ connected", ("endpoint", url));
                }
                sub.Subscribe("hashblock");
                sub.Subscribe("rawblock");

                while (!ct.IsCancellationRequested)
                {
                    if (!sub.TryReceiveFrameString(TimeSpan.FromSeconds(1), out var topic))
                        continue;

                    // Multipart: [topic][body][sequence...]
                    byte[]? body = null;
                    if (sub.HasIn)
                        sub.TryReceiveFrameBytes(out body);
                    while (sub.HasIn)
                        sub.TryReceiveFrameBytes(out _);

                    var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (now - Interlocked.Read(ref _lastZmqMs) < 10)
                        continue;
                    Interlocked.Exchange(ref _lastZmqMs, now);

                    // Solo: ZMQ only schedules full GBT (race with longpoll). Empty clean is P2P-only.
                    var source = topic == "rawblock" ? TemplateSource.ZmqRawblock : TemplateSource.ZmqHashblock;
                    string? hashHex = null;
                    if (topic == "hashblock" && body is { Length: 32 })
                    {
                        var hashBe = Hex.ReverseCopy(body);
                        hashHex = Hex.Encode(hashBe);
                    }

                    SoloLog.Info("ZMQ block notification received",
                        ("source", SoloLog.SourceName(source)),
                        ("hash", hashHex ?? ""),
                        ("action", "scheduling template refresh"));
                    _ = RefreshFromZmqAsync(source, ct);
                }
            }
            catch (Exception ex)
            {
                SoloLog.Warn("ZMQ loop error; reconnecting", ("error", ex.Message));
                Thread.Sleep(2000);
            }
        }
    }

    public Task RefreshDirectAsync(TemplateSource source, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Task completion;
        CancellationToken workerToken = CancellationToken.None;
        var startWorker = false;
        lock (_directRefreshLock)
        {
            if (!_directRefreshRunning)
            {
                _directRefreshRunning = true;
                _directCurrentSource = source;
                _directCurrentCompletion = NewDirectRefreshCompletion();
                workerToken = _hasLifetimeToken ? _lifetimeToken : CancellationToken.None;
                startWorker = true;
                completion = _directCurrentCompletion.Task;
            }
            else
            {
                _directPendingSource = source;
                if (!_directRefreshPending)
                {
                    _directRefreshPending = true;
                    _directPendingCompletion = NewDirectRefreshCompletion();
                }
                completion = _directPendingCompletion!.Task;
            }
        }

        if (startWorker)
            _ = RunDirectRefreshLoopAsync(workerToken);
        return ct.CanBeCanceled ? completion.WaitAsync(ct) : completion;
    }

    private async Task RunDirectRefreshLoopAsync(CancellationToken ct)
    {
        try
        {
            while (true)
            {
                TemplateSource source;
                lock (_directRefreshLock)
                    source = _directCurrentSource;

                Exception? error = null;
                try
                {
                    await RefreshDirectOnceAsync(source, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    StopDirectRefresh(canceled: true, null, ct);
                    return;
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                TaskCompletionSource completed;
                var runTrailing = false;
                lock (_directRefreshLock)
                {
                    completed = _directCurrentCompletion!;
                    if (_directRefreshPending)
                    {
                        _directCurrentSource = _directPendingSource;
                        _directCurrentCompletion = _directPendingCompletion;
                        _directRefreshPending = false;
                        _directPendingCompletion = null;
                        runTrailing = true;
                    }
                    else
                    {
                        _directRefreshRunning = false;
                        _directCurrentCompletion = null;
                    }
                }

                CompleteDirectRefresh(completed, error);
                if (!runTrailing)
                    return;
            }
        }
        catch (Exception ex)
        {
            StopDirectRefresh(canceled: false, ex, ct);
        }
    }

    private async Task RefreshDirectOnceAsync(TemplateSource source, CancellationToken ct)
    {
        var appliedGeneration = GetAppliedGbtGeneration();
        try
        {
            // RPC I/O is deliberately outside the publication semaphore.
            var gbt = await _rpc.CallAsync<GbtResponse>(
                "getblocktemplate", BuildGbtParams(null), ct).ConfigureAwait(false);
            await ApplyGbtAsync(gbt, source, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            ForgetAppliedGbtRevision(appliedGeneration);
            MarkRefreshFailed();
            throw;
        }
    }

    private static TaskCompletionSource NewDirectRefreshCompletion() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static void CompleteDirectRefresh(
        TaskCompletionSource completion,
        Exception? error)
    {
        if (error != null)
            completion.TrySetException(error);
        else
            completion.TrySetResult();
    }

    private void StopDirectRefresh(
        bool canceled,
        Exception? error,
        CancellationToken cancellationToken)
    {
        TaskCompletionSource? current;
        TaskCompletionSource? pending;
        lock (_directRefreshLock)
        {
            _directRefreshRunning = false;
            _directRefreshPending = false;
            current = _directCurrentCompletion;
            pending = _directPendingCompletion;
            _directCurrentCompletion = null;
            _directPendingCompletion = null;
        }

        if (canceled)
        {
            current?.TrySetCanceled(cancellationToken);
            pending?.TrySetCanceled(cancellationToken);
        }
        else if (error != null)
        {
            current?.TrySetException(error);
            pending?.TrySetException(error);
        }
        else
        {
            current?.TrySetResult();
            pending?.TrySetResult();
        }
    }

    private async Task RefreshFromZmqAsync(TemplateSource source, CancellationToken ct)
    {
        try
        {
            await RefreshDirectAsync(source, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            SoloLog.Warn("ZMQ template refresh failed",
                ("source", SoloLog.SourceName(source)),
                ("error", ex.Message));
        }
    }

    private async Task ApplyGbtAsync(
        GbtResponse gbt,
        TemplateSource source,
        CancellationToken ct)
    {
        await _gbtBuildSem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var gbtPrev = gbt.PreviousBlockhash ?? "";
            var gbtHeight = gbt.Height;
            var txCount = gbt.TransactionCount;

            // Cheap tip checks on raw GBT fields avoid coinbase, Merkle, and tx-list work.
            lock (_jobLock)
            {
                if (IsExactAppliedGbtResponse(
                        gbt, _lastAppliedGbtLongpollId, _lastAppliedGbtScalarIdentity))
                {
                    MarkRefreshOk();
                    SoloLog.Debug("skip_job",
                        ("reason", "identical_gbt_revision"),
                        ("source", SoloLog.SourceName(source)),
                        ("height", gbtHeight),
                        ("txs", txCount));
                    return;
                }

                if (IsSupersededGbtResponse(gbt, _authoritative, _lastAppliedGbtLongpollId))
                {
                    var responseRevision = ParseGbtLongpollRevision(gbt.LongPollId);
                    var appliedRevision = ParseGbtLongpollRevision(_lastAppliedGbtLongpollId);
                    SoloLog.Info("skip_job",
                        ("reason", "older_same_tip_gbt_revision"),
                        ("source", SoloLog.SourceName(source)),
                        ("gbt_height", gbtHeight),
                        ("prevhash", gbtPrev),
                        ("response_revision", responseRevision?.TransactionsUpdated),
                        ("applied_revision", appliedRevision?.TransactionsUpdated));
                    MarkRefreshOk();
                    return;
                }

                var tipRelation = ClassifyGbtTip(_active, gbtPrev, gbtHeight);
                if (tipRelation == GbtTipRelation.Behind)
                {
                    SoloLog.Debug("skip_job",
                        ("reason", "behind_tip"),
                        ("source", SoloLog.SourceName(source)),
                        ("gbt_height", gbtHeight),
                        ("current_height", _active.Height));
                    MarkRefreshOk();
                    return;
                }

                if (tipRelation == GbtTipRelation.Reorg)
                {
                    SoloLog.Info("same-height chain reorg detected",
                        ("source", SoloLog.SourceName(source)),
                        ("height", gbtHeight),
                        ("old_prev", _active.PrevhashBe),
                        ("new_prev", gbtPrev.ToLowerInvariant()));
                }
            }

            // A revision miss hashes txids once. A matching key skips full job construction.
            var keyParts = _builder.ComputeTemplateKeyParts(gbt);

            lock (_jobLock)
            {
                if (string.Equals(_lastTemplateKey, keyParts.Key, StringComparison.Ordinal))
                {
                    if (!string.IsNullOrEmpty(gbt.LongPollId))
                        _longpollId = gbt.LongPollId;
                    RecordAppliedGbtRevision(gbt);
                    MarkRefreshOk();
                    SoloLog.Debug("skip_job",
                        ("reason", "identical_template"),
                        ("source", SoloLog.SourceName(source)),
                        ("height", gbtHeight),
                        ("txs", txCount));
                    return;
                }
            }

            var job = _builder.FromGbt(gbt, source, keyParts.Key, keyParts.TxHashesLe);

            // Expensive construction stays outside the P2P publication semaphore.
            await _refreshSem.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                bool clean;
                lock (_jobLock)
                {
                    // P2P may have advanced while this candidate was being built.
                    if (IsSupersededGbtResponse(
                            gbt, _authoritative, _lastAppliedGbtLongpollId) ||
                        ClassifyGbtTip(_active, gbtPrev, gbtHeight) == GbtTipRelation.Behind)
                    {
                        MarkRefreshOk();
                        return;
                    }

                    if (string.Equals(_lastTemplateKey, job.TemplateKey, StringComparison.Ordinal))
                    {
                        if (!string.IsNullOrEmpty(gbt.LongPollId))
                            _longpollId = gbt.LongPollId;
                        RecordAppliedGbtRevision(gbt);
                        MarkRefreshOk();
                        SoloLog.Debug("skip_job",
                            ("reason", "identical_template"),
                            ("source", SoloLog.SourceName(source)),
                            ("height", job.Height),
                            ("txs", job.TransactionCount));
                        return;
                    }

                    if (!string.IsNullOrEmpty(gbt.LongPollId))
                        _longpollId = gbt.LongPollId;
                    clean = ShouldCleanGbtUpdate(_active, job);
                }

                // Publish first so miners see the job ASAP; network hashrate is dashboard-only.
                PublishAuthoritativeJob(job, clean);
                // Wake a candidate deferred on a P2P-announced parent as soon as Core catches up.
                _submitQueue.NotifyChainStateChanged();
                lock (_jobLock)
                {
                    RecordAppliedGbtRevision(gbt);
                    _tip = new ChainTip
                    {
                        HashHex = job.PrevhashBe,
                        Height = job.Height > 0 ? job.Height - 1 : 0,
                        MedianTimePast = job.Mintime > 0 ? job.Mintime - 1 : job.Ntime,
                        Nbits = job.Nbits,
                        Version = job.Version,
                        Vbrequired = job.Vbrequired,
                        TargetLe = job.TargetLe.ToArray(),
                        NetworkDifficulty = job.NetworkDifficulty
                    };
                }

                var eventName = clean ? "new block" : "template update";
                SoloLog.Info(SoloLog.FormatTemplateEvent(
                    eventName, source, job.JobId, job.Height, job.TransactionCount,
                    job.CoinbaseValue, job.Nbits, clean));

                // At most one dashboard-only RPC every 30 seconds, off notify latency.
                ScheduleNetworkHashrateRefresh();
            }
            finally
            {
                _refreshSem.Release();
            }
        }
        finally
        {
            _gbtBuildSem.Release();
        }
    }

    private void RecordAppliedGbtRevision(GbtResponse gbt)
    {
        _lastAppliedGbtLongpollId = ParseGbtLongpollRevision(gbt.LongPollId).HasValue
            ? gbt.LongPollId
            : null;
        _lastAppliedGbtScalarIdentity = _lastAppliedGbtLongpollId != null
            ? GbtScalarIdentity.FromResponse(gbt)
            : null;
        _appliedGbtGeneration++;
    }

    private long GetAppliedGbtGeneration()
    {
        lock (_jobLock)
            return _appliedGbtGeneration;
    }

    private void ForgetAppliedGbtRevision(long expectedGeneration)
    {
        // Bitcoin Core's mempool update counter restarts with the node process.
        // A failed outstanding GBT is the process-boundary signal available here.
        // Do not erase a newer revision concurrently applied by another RPC path.
        lock (_jobLock)
        {
            if (_appliedGbtGeneration != expectedGeneration)
                return;
            _lastAppliedGbtLongpollId = null;
            _lastAppliedGbtScalarIdentity = null;
            _appliedGbtGeneration++;
        }
    }

    internal static bool IsExactAppliedGbtResponse(
        GbtResponse response,
        string? lastAppliedLongpollId,
        GbtScalarIdentity? lastAppliedIdentity) =>
        !string.IsNullOrEmpty(response.LongPollId) &&
        string.Equals(response.LongPollId, lastAppliedLongpollId, StringComparison.Ordinal) &&
        lastAppliedIdentity.HasValue &&
        lastAppliedIdentity.Value == GbtScalarIdentity.FromResponse(response);

    internal static bool IsSupersededGbtResponse(
        GbtResponse response,
        JobTemplate authoritative,
        string? lastAppliedLongpollId) =>
        IsSupersededGbtResponse(
            response,
            authoritative.Ready,
            authoritative.Height,
            authoritative.PrevhashBe,
            lastAppliedLongpollId);

    private static bool IsSupersededGbtResponse(
        GbtResponse response,
        AuthoritativeJobSnapshot authoritative,
        string? lastAppliedLongpollId) =>
        IsSupersededGbtResponse(
            response,
            authoritative.Ready,
            authoritative.Height,
            authoritative.PrevhashBe,
            lastAppliedLongpollId);

    private static bool IsSupersededGbtResponse(
        GbtResponse response,
        bool authoritativeReady,
        uint authoritativeHeight,
        string authoritativePrevhash,
        string? lastAppliedLongpollId)
    {
        if (!authoritativeReady || response.Height != authoritativeHeight ||
            !string.Equals(
                response.PreviousBlockhash,
                authoritativePrevhash,
                StringComparison.OrdinalIgnoreCase))
            return false;

        var responseRevision = ParseGbtLongpollRevision(response.LongPollId);
        var appliedRevision = ParseGbtLongpollRevision(lastAppliedLongpollId);
        return responseRevision.HasValue && appliedRevision.HasValue &&
            string.Equals(
                responseRevision.Value.TipHash,
                response.PreviousBlockhash,
                StringComparison.OrdinalIgnoreCase) &&
            string.Equals(
                appliedRevision.Value.TipHash,
                responseRevision.Value.TipHash,
                StringComparison.OrdinalIgnoreCase) &&
            responseRevision.Value.TransactionsUpdated < appliedRevision.Value.TransactionsUpdated;
    }

    internal static GbtLongpollRevision? ParseGbtLongpollRevision(string? longpollId)
    {
        // Bitcoin Core format: <64-char best-chain hash><mempool update counter>.
        if (string.IsNullOrWhiteSpace(longpollId) || longpollId.Length <= 64)
            return null;

        var tipHash = longpollId[..64];
        if (!Hex.TryDecode(tipHash, out var tipBytes) || tipBytes.Length != 32 ||
            !ulong.TryParse(
                longpollId.AsSpan(64),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var transactionsUpdated))
            return null;

        return new GbtLongpollRevision(tipHash.ToLowerInvariant(), transactionsUpdated);
    }

    private void MarkRefreshOk()
    {
        _metrics.LastRefreshOk = true;
        _metrics.LastRefreshMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    private void MarkRefreshFailed()
    {
        _metrics.LastRefreshOk = false;
    }

    private void ScheduleNetworkHashrateRefresh()
    {
        CancellationToken ct;
        lock (_directRefreshLock)
            ct = _hasLifetimeToken ? _lifetimeToken : CancellationToken.None;

        lock (_networkHashrateLock)
        {
            var now = Environment.TickCount64;
            if (_networkHashrateTask is { IsCompleted: false } ||
                now < _nextNetworkHashrateRefreshMs)
                return;

            _nextNetworkHashrateRefreshMs = now + 30_000;
            _networkHashrateTask = RefreshNetworkHashrateAsync(ct);
        }
    }

    /// <summary>Best-effort dashboard metric; must not delay stratum job notify.</summary>
    private async Task RefreshNetworkHashrateAsync(CancellationToken ct)
    {
        try
        {
            var nh = await _rpc.CallAsync<double>("getnetworkhashps", Array.Empty<object>(), ct);
            _metrics.NetworkHashrateHps = nh;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch
        {
            // ignore — stale network hashrate is acceptable
        }
    }

    private void PublishFastJob(JobTemplate job) =>
        PublishJob(job, cleanJobs: true, authoritative: false);

    private void PublishAuthoritativeJob(JobTemplate job, bool cleanJobs) =>
        PublishJob(job, cleanJobs, authoritative: true);

    private void PublishJob(JobTemplate job, bool cleanJobs, bool authoritative)
    {
        JobNotify notify;
        lock (_publicationLock)
        {
            // A pending clean flag follows the latest coalesced notification. This is
            // required when a P2P-fast clean job is superseded by its same-tip full GBT
            // before the Stratum broadcaster observes the fast notification.
            var clean = cleanJobs || (_pendingNotify?.CleanJobs ?? false);
            var epoch = Interlocked.Increment(ref _jobEpoch);
            job.Epoch = epoch;
            lock (_jobLock)
            {
                _active = job;
                if (authoritative)
                    _authoritative = AuthoritativeJobSnapshot.FromJob(job);
                _lastTemplateKey = job.TemplateKey;
                if (clean)
                {
                    foreach (var entry in _jobs)
                    {
                        if (!entry.RetiredByEpoch.HasValue)
                        {
                            entry.RetiredByEpoch = epoch;
                            entry.RetiredAt = DateTimeOffset.UtcNow;
                        }
                    }
                }
                var registration = new JobRegistration(job);
                _jobs.Insert(0, registration);
                _retainedTransactionBytes = checked(
                    _retainedTransactionBytes + job.TransactionBytes);
                if (job.JobKey != 0)
                    _jobsByKey[job.JobKey] = registration;
                TrimNonRetiredJobsLocked();
                ReclaimJobsLocked(DateTimeOffset.UtcNow);
                // Publishing a job changes the lookup even when reclaim itself is a no-op:
                // the new key and any clean-retired flags must become visible atomically.
                PublishJobLookupSnapshotLocked();
            }

            if (authoritative)
                MarkRefreshOk();
            notify = new JobNotify(epoch, job, clean);
            _pendingNotify = notify;
        }
        _notifySignal.Writer.TryWrite(true);
    }

    private void TrimNonRetiredJobsLocked()
    {
        var keep = Math.Max(1, _cfg.Runtime.KeepOldJobs + 1);
        var nonRetired = 0;
        for (var i = 0; i < _jobs.Count;)
        {
            var entry = _jobs[i];
            if (entry.RetiredByEpoch.HasValue)
            {
                i++;
                continue;
            }

            nonRetired++;
            if (nonRetired <= keep || ReferenceEquals(entry.Job, _active))
            {
                i++;
                continue;
            }

            RemoveJobAtLocked(i, DateTimeOffset.UtcNow);
        }
    }

    private bool ReclaimJobsLocked(DateTimeOffset now)
    {
        var grace = TimeSpan.FromMilliseconds(_cfg.Stratum.LateShareGraceMs);
        var maxAge = TimeSpan.FromSeconds(_cfg.Runtime.RetiredJobMaxAgeSecs);
        var retiredCount = 0;
        for (var i = 0; i < _jobs.Count; i++)
        {
            if (_jobs[i].RetiredByEpoch.HasValue)
                retiredCount++;
        }
        var lookupChanged = false;

        for (var i = _jobs.Count - 1; i >= 0; i--)
        {
            var entry = _jobs[i];
            if (ReferenceEquals(entry.Job, _active))
                continue;

            // The byte budget covers every retained transaction set, regardless of
            // clean/retired state. Walk oldest-first and preserve only the active job.
            if (_retainedTransactionBytes > _cfg.Runtime.MaxRetainedTransactionBytes)
            {
                lookupChanged |= RemoveJobAtLocked(i, now);
                if (entry.RetiredByEpoch.HasValue)
                    retiredCount--;
                continue;
            }
            if (!entry.RetiredByEpoch.HasValue)
                continue;

            var barrierAndGraceComplete = entry.BroadcastCompletedAt.HasValue &&
                now - entry.BroadcastCompletedAt.Value >= grace;
            var hardAgeExceeded = entry.RetiredAt.HasValue && now - entry.RetiredAt.Value >= maxAge;
            var overMemoryBound = retiredCount > _cfg.Runtime.MaxRetiredJobs;
            if (!barrierAndGraceComplete && !hardAgeExceeded && !overMemoryBound)
                continue;

            lookupChanged |= RemoveJobAtLocked(i, now);
            retiredCount--;
        }

        RemoveExpiredTombstones(_expiredJobIds, now);
        lookupChanged |= RemoveExpiredTombstones(_expiredJobKeys, now);
        return lookupChanged;
    }

    private void PublishJobLookupSnapshotLocked()
    {
        var jobs = new JobLookupSnapshotEntry[_jobsByKey.Count];
        var index = 0;
        foreach (var pair in _jobsByKey)
        {
            jobs[index++] = new JobLookupSnapshotEntry(
                pair.Key, pair.Value.Job, pair.Value.RetiredByEpoch.HasValue);
        }

        var expired = new ExpiredJobLookupEntry[_expiredJobKeys.Count];
        index = 0;
        foreach (var pair in _expiredJobKeys)
            expired[index++] = new ExpiredJobLookupEntry(pair.Key, pair.Value);

        Volatile.Write(ref _jobLookupSnapshot, new JobLookupSnapshot(jobs, expired));
        Interlocked.Increment(ref _jobLookupSnapshotPublicationCount);
    }

    private bool RemoveJobAtLocked(int index, DateTimeOffset now)
    {
        var job = _jobs[index].Job;
        RememberExpiredLocked(job, now);
        if (job.JobKey != 0)
            _jobsByKey.Remove(job.JobKey);
        _retainedTransactionBytes -= job.TransactionBytes;
        if (_retainedTransactionBytes < 0)
            _retainedTransactionBytes = 0;
        _jobs.RemoveAt(index);
        return job.JobKey != 0;
    }

    private static bool RemoveExpiredTombstones<TKey>(
        Dictionary<TKey, DateTimeOffset> tombstones,
        DateTimeOffset cutoff)
        where TKey : notnull
    {
        List<TKey>? expiredKeys = null;
        foreach (var pair in tombstones)
        {
            if (pair.Value <= cutoff)
                (expiredKeys ??= new List<TKey>()).Add(pair.Key);
        }

        if (expiredKeys == null)
            return false;

        foreach (var key in expiredKeys)
            tombstones.Remove(key);
        return true;
    }

    private void RememberExpiredLocked(JobTemplate job, DateTimeOffset now)
    {
        var tombstoneLifetime = TimeSpan.FromSeconds(
            Math.Max(_cfg.Runtime.RetiredJobMaxAgeSecs, 5));
        var expiresAt = now + tombstoneLifetime;
        _expiredJobIds[job.JobId] = expiresAt;
        if (job.JobKey != 0)
            _expiredJobKeys[job.JobKey] = expiresAt;
    }

    internal static bool ShouldCleanGbtUpdate(JobTemplate active, JobTemplate next)
    {
        if (!active.Ready)
            return true;

        var chainChanged = active.Height != next.Height ||
            !string.Equals(active.PrevhashBe, next.PrevhashBe, StringComparison.OrdinalIgnoreCase);
        if (chainChanged)
            return true;

        // submitold describes replacement of Core's preceding GBT. A P2P-fast
        // job already mines on this exact tip, so confirming it must preserve
        // late fast-job shares and take over with clean=false.
        return !next.SubmitOld && active.Source != TemplateSource.P2pFast;
    }

    private static object[] BuildGbtParams(string? longpollId)
    {
        var rules = new Dictionary<string, object>
        {
            ["rules"] = new[] { "segwit" }
        };
        if (!string.IsNullOrEmpty(longpollId))
            rules["longpollid"] = longpollId;
        return new object[] { rules };
    }

    internal static string? ValidateP2pFastParentHeader(
        string networkName,
        ChainTip tip,
        string prevhashHex,
        string blockHashHex,
        uint blockTime,
        uint blockHeight,
        uint blockNbits,
        uint blockVersion,
        long nowUnixTime)
    {
        if (!networkName.Equals("mainnet", StringComparison.OrdinalIgnoreCase) &&
            !networkName.Equals("bitcoin", StringComparison.OrdinalIgnoreCase))
            return "unsupported_network";
        if (!IsHash256Hex(prevhashHex) || !IsHash256Hex(blockHashHex))
            return "malformed_header_hash";
        if (!tip.HashHex.Equals(prevhashHex, StringComparison.OrdinalIgnoreCase))
            return "prevhash_mismatch";
        if (tip.Height == uint.MaxValue || blockHeight != tip.Height + 1)
            return "height_mismatch";
        if (blockHeight == uint.MaxValue)
            return "height_overflow";
        if (blockNbits != tip.Nbits)
            return "nbits_mismatch";

        Span<byte> compactTargetLe = stackalloc byte[32];
        if (!BitcoinEncoding.TryCompactTargetToLe(blockNbits, compactTargetLe))
            return "invalid_compact_target";
        if (tip.TargetLe.Length != 32 || !tip.TargetLe.AsSpan().SequenceEqual(compactTargetLe))
            return "tip_target_mismatch";
        if (blockTime <= tip.MedianTimePast)
            return "time_too_old";
        if (nowUnixTime < 0 || (long)blockTime - nowUnixTime > 2 * 60 * 60)
            return "time_too_new";

        var signedVersion = unchecked((int)blockVersion);
        var minimumVersion = blockHeight >= 388_381 ? 4 :
            blockHeight >= 363_725 ? 3 :
            blockHeight >= 227_931 ? 2 : int.MinValue;
        if (signedVersion < minimumVersion)
            return "obsolete_version";
        if ((blockVersion & tip.Vbrequired) != tip.Vbrequired)
            return "missing_required_version_bits";
        if (!BlockHashMeetsTarget(blockHashHex, compactTargetLe))
            return "invalid_pow";
        return null;
    }

    internal static uint ConservativeMtpUpperBound(uint previousMtp, uint parentTime) =>
        Math.Max(previousMtp, parentTime);

    private static bool IsHash256Hex(string value)
    {
        if (value == null || value.Length != 64)
            return false;
        Span<byte> hash = stackalloc byte[32];
        return BitcoinEncoding.TryDecodeExactHex(value.AsSpan(), hash);
    }

    private static bool BlockHashMeetsTarget(string hashHex, ReadOnlySpan<byte> targetLe)
    {
        try
        {
            var hashBe = Hex.Decode(hashHex);
            if (hashBe.Length != 32) return false;
            var hashLe = Hex.ReverseCopy(hashBe);
            return BitcoinEncoding.LeqLe256(hashLe, targetLe);
        }
        catch { return false; }
    }

    internal static GbtTipRelation ClassifyGbtTip(JobTemplate current, string gbtPrev, uint gbtHeight)
    {
        // Job.Height is the block being mined, while PrevhashBe is the active tip.
        // A P2P-fast announcement for block H publishes a speculative job for H+1.
        // Therefore an older Core GBT still mining H is Behind, not a same-height
        // reorg. Core confirming H on another hash produces an H+1 GBT and is the
        // genuine same-height reorg case.
        if (!current.Ready)
            return GbtTipRelation.Ahead;
        if (gbtHeight < current.Height)
            return GbtTipRelation.Behind;
        if (gbtHeight > current.Height)
            return GbtTipRelation.Ahead;
        return string.Equals(current.PrevhashBe, gbtPrev, StringComparison.OrdinalIgnoreCase)
            ? GbtTipRelation.SameTip
            : GbtTipRelation.Reorg;
    }
}

public readonly record struct JobNotify(long Epoch, JobTemplate Job, bool CleanJobs);

public readonly record struct AuthoritativeJobSnapshot(
    bool Ready,
    long Epoch,
    uint Height,
    long CoinbaseValue,
    int TransactionCount,
    double NetworkDifficulty,
    string PrevhashBe)
{
    public static AuthoritativeJobSnapshot Empty { get; } =
        new(false, 0, 0, 0, 0, 0, "");

    internal static AuthoritativeJobSnapshot FromJob(JobTemplate job) =>
        new(
            job.Ready,
            job.Epoch,
            job.Height,
            job.CoinbaseValue,
            job.TransactionCount,
            job.NetworkDifficulty,
            job.PrevhashBe);
}

internal readonly record struct GbtLongpollRevision(string TipHash, ulong TransactionsUpdated);

internal readonly record struct GbtScalarIdentity(
    uint Version,
    string PreviousBlockhash,
    long CoinbaseValue,
    string Target,
    uint CurTime,
    string Bits,
    uint Height,
    int TransactionCount,
    string? CoinbaseAuxFlags,
    string? WitnessCommitment,
    uint Mintime,
    uint Vbrequired,
    bool SubmitOld)
{
    public static GbtScalarIdentity FromResponse(GbtResponse response) =>
        new(
            response.Version,
            response.PreviousBlockhash,
            response.CoinbaseValue,
            response.Target,
            response.CurTime,
            response.Bits,
            response.Height,
            response.TransactionCount,
            response.CoinbaseAux?.Flags,
            response.DefaultWitnessCommitment,
            response.Mintime ?? response.CurTime,
            response.Vbrequired,
            response.SubmitOld ?? true);
}

internal sealed class JobLookupSnapshot
{
    public static JobLookupSnapshot Empty { get; } =
        new(Array.Empty<JobLookupSnapshotEntry>(), Array.Empty<ExpiredJobLookupEntry>());

    public JobLookupSnapshot(
        JobLookupSnapshotEntry[] jobs,
        ExpiredJobLookupEntry[] expired)
    {
        Jobs = jobs;
        Expired = expired;
    }

    public JobLookupSnapshotEntry[] Jobs { get; }
    public ExpiredJobLookupEntry[] Expired { get; }
}

internal readonly record struct JobLookupSnapshotEntry(
    ulong JobKey,
    JobTemplate Job,
    bool Retired);

internal readonly record struct ExpiredJobLookupEntry(
    ulong JobKey,
    DateTimeOffset ExpiresAt);

public readonly record struct JobLookupResult(JobLookupStatus Status, JobTemplate? Job);

public enum JobLookupStatus
{
    Available,
    RetiredWithinGrace,
    Expired,
    Unknown
}

internal sealed class JobRegistration(JobTemplate job)
{
    public JobTemplate Job { get; } = job;
    public long? RetiredByEpoch { get; set; }
    public DateTimeOffset? RetiredAt { get; set; }
    public DateTimeOffset? BroadcastCompletedAt { get; set; }
}

internal enum GbtTipRelation
{
    Behind,
    SameTip,
    Reorg,
    Ahead
}
