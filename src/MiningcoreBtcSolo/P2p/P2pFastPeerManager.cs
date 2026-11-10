using System.Diagnostics;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.P2p;

public sealed class P2pFastPeerManager
{
    private const int MaxPeers = 128;
    private const int MaxAutoPeers = 128;
    private const int MaxAutoHistoryRecords = 4096;
    private const int AutoAddressQueryCount = 1000;
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan AutoDiscoveryInterval = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan PeerConnectionRetryLimit = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan PeerRetryTimeoutScanInterval = TimeSpan.FromSeconds(5);
    private readonly AppConfig _cfg;
    private readonly BitcoinRpcClient _rpc;
    private readonly P2pFastPeer _client;
    private readonly string _peerFilePath;
    private readonly string _autoStateFilePath;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly SemaphoreSlim _autoRefreshSignal = new(0, 1);
    private readonly ConcurrentDictionary<string, PeerRuntime> _managed = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, PeerRuntime> _autoManaged = new(StringComparer.OrdinalIgnoreCase);
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentDictionary<string, byte> _blacklistedPeers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, PersistedAutoPeerRecord> _autoHistory = new(StringComparer.OrdinalIgnoreCase);
    private readonly PeerRuntime? _configuredRuntime;
    private readonly string? _configuredPeer;
    private string[] _managedPeers = Array.Empty<string>();
    private P2pFastAutoSettings _autoSettings = P2pFastAutoSettings.Default;
    private int _autoCandidateCount;
    private long _lastAutoDiscoveryMs;
    private string? _autoDiscoveryError;
    private CancellationToken _serviceToken;
    private int _started;
    private readonly bool _managementEnabled;
    private readonly bool _enabled;

    public P2pFastPeerManager(AppConfig cfg, TemplateEngine engine, BitcoinRpcClient rpc)
    {
        _cfg = cfg;
        _rpc = rpc;
        _client = new P2pFastPeer(cfg, engine, rpc);
        _managementEnabled = Guid.TryParse(cfg.Api.P2pFastPeerUuid, out _);
        try
        {
            _configuredPeer = string.IsNullOrWhiteSpace(cfg.Bitcoind.P2pFastPeer)
                ? null
                : NormalizeConfiguredPeer(cfg.Bitcoind.P2pFastPeer, cfg.NetworkName);
        }
        catch (ArgumentException ex)
        {
            SoloLog.Warn("invalid p2p fast peer ignored", ("error", ex.Message));
        }
        if (_configuredPeer != null)
            _configuredRuntime = new PeerRuntime(_configuredPeer, exemptFromRetryLimit: true);

        _enabled = _managementEnabled || _configuredPeer != null;
        if (_enabled)
        {
            _client.PeerStateChanged += OnPeerStateChanged;
            _client.PeerTcpConnected += OnPeerTcpConnected;
            engine.P2pFastBlockPublished += OnFastBlockPublished;
        }

        var dataDir = string.IsNullOrWhiteSpace(cfg.Runtime.DataDir) ? "data" : cfg.Runtime.DataDir;
        var fullDataDir = Path.IsPathRooted(dataDir)
            ? dataDir
            : Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), dataDir));
        _peerFilePath = Path.Combine(fullDataDir, "p2p-fast-peers.json");
        _autoStateFilePath = Path.Combine(fullDataDir, "p2p-fast-auto.json");
    }

    public string PeerFilePath => _peerFilePath;
    public bool IsEnabled => _enabled;

    public async Task RunAsync(CancellationToken ct)
    {
        if (!_enabled)
            return;
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("P2P-fast peer manager can only be started once");

        _serviceToken = ct;
        Task? autoMaintenanceTask = null;
        Task? peerTimeoutPrunerTask = null;
        try
        {
            await InitializeAsync(ct);
            _ready.TrySetResult();
            if (_managementEnabled)
                peerTimeoutPrunerTask = PeerRetryTimeoutPrunerLoopAsync(ct);
            if (IsMainnet && _managementEnabled)
                autoMaintenanceTask = AutoPeerMaintenanceLoopAsync(ct);
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _ready.TrySetCanceled(ct);
        }
        catch (Exception ex)
        {
            _ready.TrySetException(ex);
            throw;
        }
        finally
        {
            if (peerTimeoutPrunerTask != null)
            {
                try { await peerTimeoutPrunerTask; }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            }
            if (autoMaintenanceTask != null)
            {
                try { await autoMaintenanceTask; }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            }
            await StopAllAsync();
        }
    }

    public async Task<IReadOnlyList<P2pFastPeerSnapshot>> GetPeersAsync(CancellationToken ct = default)
    {
        await WaitUntilReadyAsync(ct);
        var result = new List<P2pFastPeerSnapshot>(_managed.Count + _autoManaged.Count + 1);
        result.AddRange(_managed.Values.Select(entry => entry.Snapshot(
            isManaged: true, isConfigured: false, isAuto: false, isActive: true)));
        result.AddRange(_autoManaged.Values.Select(entry => entry.Snapshot(
            isManaged: false, isConfigured: false, isAuto: true, isActive: true)));

        if (_configuredRuntime != null)
            result.Add(_configuredRuntime.Snapshot(
                isManaged: false,
                isConfigured: true,
                isAuto: false,
                isActive: true));

        var rank = 0;
        foreach (var peer in result.Where(peer => peer.FastBlockCount > 0)
                     .OrderByDescending(peer => peer.FastBlockCount)
                     .ThenByDescending(peer => peer.LastFastBlockMs))
            peer.FastRank = ++rank;

        return result.OrderBy(peer => peer.Peer, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public async Task<P2pFastAutoStatus> GetAutoStatusAsync(CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        await WaitUntilReadyAsync(ct);
        return new P2pFastAutoStatus(
            Volatile.Read(ref _autoSettings),
            IsMainnet,
            _autoManaged.Count,
            _blacklistedPeers.Count,
            _blacklistedPeers.Keys.Take(20).ToArray(),
            Volatile.Read(ref _autoCandidateCount),
            Interlocked.Read(ref _lastAutoDiscoveryMs),
            Volatile.Read(ref _autoDiscoveryError));
    }

    public async Task<P2pFastAutoStatus> UpdateAutoSettingsAsync(
        P2pFastAutoSettings settings,
        CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        ArgumentNullException.ThrowIfNull(settings);
        ValidateAutoSettings(settings);
        if (settings.Enabled && !IsMainnet)
            throw new InvalidOperationException("Automatic peer discovery is available only on mainnet");
        await WaitUntilReadyAsync(ct);

        await _mutationGate.WaitAsync(ct);
        try
        {
            Volatile.Write(ref _autoSettings, settings);
            await SaveAutoStateAsync(ct);
        }
        finally
        {
            _mutationGate.Release();
        }

        if (!settings.Enabled)
            await DisableAutoPeersAsync(_serviceToken);
        else
            RequestAutoRefresh();

        return await GetAutoStatusAsync(ct);
    }

    public async Task<P2pFastAutoStatus> ClearBlacklistAsync(CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        await WaitUntilReadyAsync(ct);

        await _mutationGate.WaitAsync(ct);
        try
        {
            var removed = _blacklistedPeers.Keys.ToArray();
            if (removed.Length == 0)
                return await GetAutoStatusAsync(ct);

            foreach (var peer in removed)
                _blacklistedPeers.TryRemove(peer, out _);

            try
            {
                await SaveAutoStateAsync(ct);
            }
            catch
            {
                foreach (var peer in removed)
                    _blacklistedPeers.TryAdd(peer, 0);
                try { await SaveAutoStateAsync(CancellationToken.None); }
                catch (Exception ex)
                {
                    SoloLog.Warn("failed to restore p2p peer blacklist after clear failure", ("error", ex.Message));
                }
                throw;
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        if (IsMainnet && Volatile.Read(ref _autoSettings).Enabled)
            RequestAutoRefresh();
        return await GetAutoStatusAsync(ct);
    }

    public async Task<string[]> ExportManagedPeersAsync(CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        await WaitUntilReadyAsync(ct);
        return Volatile.Read(ref _managedPeers).ToArray();
    }

    public async Task AddPeerAsync(string peer, CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        var normalized = NormalizeManagedPeer(peer, _cfg.NetworkName);
        EnsureNotConfiguredPeer(normalized);
        await WaitUntilReadyAsync(ct);
        await _mutationGate.WaitAsync(ct);
        try
        {
            var current = _managedPeers;
            if (current.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                throw new InvalidOperationException("Peer is already configured");
            if (current.Length >= MaxPeers)
                throw new InvalidOperationException($"A maximum of {MaxPeers} peers is allowed");

            var autoEntry = _autoManaged.GetValueOrDefault(normalized);
            var updated = current.Append(normalized).ToArray();
            var unblacklisted = _blacklistedPeers.TryRemove(normalized, out _);
            var hadAutoHistory = _autoHistory.Remove(normalized, out var autoHistory);
            try
            {
                if (unblacklisted || hadAutoHistory)
                    await SaveAutoStateAsync(ct);
                await SavePeersAsync(updated, ct);
            }
            catch
            {
                if (unblacklisted)
                {
                    _blacklistedPeers.TryAdd(normalized, 0);
                    try { await SaveAutoStateAsync(CancellationToken.None); }
                    catch (Exception ex) { SoloLog.Warn("failed to restore p2p peer blacklist after add failure", ("peer", normalized), ("error", ex.Message)); }
                }
                if (hadAutoHistory)
                {
                    _autoHistory[normalized] = autoHistory!;
                    try { await SaveAutoStateAsync(CancellationToken.None); }
                    catch (Exception ex) { SoloLog.Warn("failed to restore automatic peer history after add failure", ("peer", normalized), ("error", ex.Message)); }
                }
                throw;
            }
            Volatile.Write(ref _managedPeers, updated);
            if (autoEntry != null && _autoManaged.TryRemove(normalized, out var promotedEntry))
            {
                _managed[normalized] = promotedEntry;
                promotedEntry.EnsureProbeRunning(token => RunProbeLoopAsync(promotedEntry, token));
            }
            else
            {
                await ReconcilePeersAsync();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    public async Task<bool> RemovePeerAsync(string peer, CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        var normalized = NormalizeManagedPeer(peer, _cfg.NetworkName);
        await WaitUntilReadyAsync(ct);
        await _mutationGate.WaitAsync(ct);
        try
        {
            var current = _managedPeers;
            var isManual = current.Contains(normalized, StringComparer.OrdinalIgnoreCase);
            var isAuto = _autoManaged.TryGetValue(normalized, out var autoEntry);
            if (!isManual && !isAuto)
                return false;

            var updated = current.Where(value =>
                !value.Equals(normalized, StringComparison.OrdinalIgnoreCase)).ToArray();
            BlacklistPeer(normalized);
            await SaveAutoStateAsync(ct);
            if (isManual)
            {
                await SavePeersAsync(updated, ct);
                Volatile.Write(ref _managedPeers, updated);
                await ReconcilePeersAsync();
            }
            if (isAuto && _autoManaged.TryRemove(normalized, out autoEntry))
                await StopRuntimeAsync(autoEntry);
            return true;
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    public async Task ImportPeersAsync(IReadOnlyList<string> peers, CancellationToken ct = default)
    {
        EnsureManagementEnabled();
        ArgumentNullException.ThrowIfNull(peers);
        if (peers.Count > MaxPeers)
            throw new ArgumentException($"A maximum of {MaxPeers} peers is allowed", nameof(peers));

        var normalized = new List<string>(peers.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var peer in peers)
        {
            var value = NormalizeManagedPeer(peer, _cfg.NetworkName);
            if (!seen.Add(value))
                throw new ArgumentException($"Duplicate peer: {value}", nameof(peers));
            normalized.Add(value);
        }

        await WaitUntilReadyAsync(ct);
        await _mutationGate.WaitAsync(ct);
        try
        {
            var updated = new List<string>(Math.Min(MaxPeers, _managedPeers.Length + normalized.Count));
            var updatedSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in _managed.Values)
            {
                if (!entry.Snapshot(true, false, true).State.Equals("connected", StringComparison.Ordinal) ||
                    _blacklistedPeers.ContainsKey(entry.Peer))
                    continue;
                updated.Add(entry.Peer);
                updatedSet.Add(entry.Peer);
            }

            foreach (var peer in normalized)
            {
                if (IsConfiguredPeer(peer) || _autoManaged.ContainsKey(peer) ||
                    _blacklistedPeers.ContainsKey(peer) || !updatedSet.Add(peer))
                    continue;
                if (updated.Count >= MaxPeers)
                    continue;
                updated.Add(peer);
            }

            var updatedPeers = updated.ToArray();
            var removed = _managedPeers.Except(updatedPeers, StringComparer.OrdinalIgnoreCase).ToArray();
            foreach (var peer in removed)
                BlacklistPeer(peer);
            if (removed.Length > 0)
                await SaveAutoStateAsync(ct);
            await SavePeersAsync(updatedPeers, ct);
            Volatile.Write(ref _managedPeers, updatedPeers);
            await ReconcilePeersAsync();
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    public async Task<double> ProbePeerAsync(string peer, CancellationToken ct = default)
    {
        var normalized = NormalizeManagedPeer(peer, _cfg.NetworkName);
        await WaitUntilReadyAsync(ct);
        var entry = FindEntry(normalized)
                    ?? throw new InvalidOperationException("Peer is not active or configured");
        try
        {
            var result = await ProbeAverageAsync(normalized, 3, ct);
            entry.SetPing(result.AverageMs, result.SuccessfulSamples, null);
            return result.AverageMs;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException)
        {
            entry.SetPing(null, 0, ex.Message);
            throw;
        }
    }

    internal static (string Host, int Port) ParseEndpoint(
        string peer,
        string network,
        bool allowDefaultPort)
    {
        var text = peer.Trim();
        string host;
        string? portText;
        if (text.StartsWith("[", StringComparison.Ordinal))
        {
            var close = text.IndexOf(']');
            if (close < 0 || close + 1 >= text.Length || text[close + 1] != ':')
                throw new ArgumentException("IPv6 peers must use [address]:port format", nameof(peer));
            host = text[1..close];
            portText = text[(close + 2)..];
            if (!IPAddress.TryParse(host, out var ip) || ip.AddressFamily != AddressFamily.InterNetworkV6)
                throw new ArgumentException("Invalid IPv6 address", nameof(peer));
            host = ip.ToString();
        }
        else
        {
            var colon = text.LastIndexOf(':');
            if (colon < 0)
            {
                if (!allowDefaultPort)
                    throw new ArgumentException("Peer must use host:port format", nameof(peer));
                host = text;
                portText = null;
            }
            else
            {
                if (text.IndexOf(':') != colon)
                    throw new ArgumentException("IPv6 peers must use [address]:port format", nameof(peer));
                host = text[..colon];
                portText = text[(colon + 1)..];
            }
        }

        if (!IsValidHost(host))
            throw new ArgumentException("Invalid peer host name or IP address", nameof(peer));
        var port = portText == null
            ? allowDefaultPort
                ? DefaultPort(network)
                : throw new ArgumentException("Peer must use host:port format", nameof(peer))
            : portText.Length > 0 && portText.All(ch => ch is >= '0' and <= '9') &&
              int.TryParse(portText, out var parsed) && parsed is >= 1 and <= 65535
                ? parsed
                : throw new ArgumentException("Peer port must be 1..65535", nameof(peer));
        return (host, port);
    }

    public static string NormalizeManagedPeer(string peer, string network)
    {
        if (string.IsNullOrWhiteSpace(peer))
            throw new ArgumentException("Peer is required", nameof(peer));
        var endpoint = ParseEndpoint(peer, network, allowDefaultPort: false);
        var host = endpoint.Host.Contains(':') ? $"[{endpoint.Host}]" : endpoint.Host.ToLowerInvariant();
        return $"{host}:{endpoint.Port}";
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        string[] peers = Array.Empty<string>();
        if (_managementEnabled)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_peerFilePath)!);
            if (File.Exists(_peerFilePath))
            {
                try
                {
                    var stored = JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(_peerFilePath, ct))
                                 ?? Array.Empty<string>();
                    peers = NormalizeStoredPeers(stored);
                }
                catch (Exception ex) when (ex is JsonException or IOException or ArgumentException)
                {
                    SoloLog.Warn("p2p fast peer file could not be loaded; configured peer remains active",
                        ("path", _peerFilePath), ("error", ex.Message));
                }
            }

            if (File.Exists(_autoStateFilePath))
            {
                try
                {
                    var persisted = JsonSerializer.Deserialize<PersistedAutoPeerState>(
                        await File.ReadAllTextAsync(_autoStateFilePath, ct), AppConfig.JsonOptions());
                    if (persisted?.Settings != null)
                    {
                        ValidateAutoSettings(persisted.Settings);
                        Volatile.Write(ref _autoSettings, persisted.Settings);
                    }
                    if (persisted?.BlacklistedPeers != null)
                    {
                        foreach (var peer in persisted.BlacklistedPeers)
                        {
                            try { _blacklistedPeers.TryAdd(NormalizeConfiguredPeer(peer, _cfg.NetworkName), 0); }
                            catch (ArgumentException) { }
                        }
                    }

                    // Old cooldown entries represented peers already rejected or evicted.
                    if (persisted?.Cooldowns != null)
                    {
                        foreach (var peer in persisted.Cooldowns)
                        {
                            try { _blacklistedPeers.TryAdd(NormalizeConfiguredPeer(peer.Peer, _cfg.NetworkName), 0); }
                            catch (ArgumentException) { }
                        }
                    }
                    if (persisted?.PeerHistory != null)
                    {
                        foreach (var record in persisted.PeerHistory)
                        {
                            var normalized = NormalizeConfiguredPeer(record.Peer, _cfg.NetworkName);
                            if (!_blacklistedPeers.ContainsKey(normalized) &&
                                record.FastBlockCount is >= 0 and <= 1_000_000_000_000 &&
                                IsUnixMilliseconds(record.LastFastBlockMs) &&
                                IsUnixMilliseconds(record.AutoSelectedMs) &&
                                record.LastFastBlockMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() &&
                                record.AutoSelectedMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                                _autoHistory[normalized] = record with { Peer = normalized };
                        }
                    }
                }
                catch (Exception ex) when (ex is JsonException or IOException or ArgumentException)
                {
                    SoloLog.Warn("automatic p2p peer state could not be loaded; defaults will be used",
                        ("path", _autoStateFilePath), ("error", ex.Message));
                }
            }

            var filteredPeers = peers.Where(peer => !_blacklistedPeers.ContainsKey(peer)).ToArray();
            if (filteredPeers.Length != peers.Length)
            {
                peers = filteredPeers;
                await SavePeersAsync(peers, ct);
            }
        }

        Volatile.Write(ref _managedPeers, peers);
        await ReconcilePeersAsync();
        SoloLog.Info("p2p fast peer manager ready",
            ("configured_peer", _configuredPeer ?? "none"),
            ("management_enabled", _managementEnabled),
            ("managed_peers", peers.Length),
            ("peer_file", _managementEnabled ? _peerFilePath : "disabled"));
    }

    private string[] NormalizeStoredPeers(IEnumerable<string> peers)
    {
        var result = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var peer in peers)
        {
            if (result.Count == MaxPeers)
            {
                SoloLog.Warn("p2p fast peer limit reached; remaining stored peers ignored",
                    ("max_peers", MaxPeers));
                break;
            }
            try
            {
                var normalized = NormalizeConfiguredPeer(peer, _cfg.NetworkName);
                if (IsConfiguredPeer(normalized))
                    continue;
                if (seen.Add(normalized))
                    result.Add(normalized);
            }
            catch (ArgumentException ex)
            {
                SoloLog.Warn("invalid p2p fast peer ignored", ("peer", peer), ("error", ex.Message));
            }
        }
        return result.ToArray();
    }

    private async Task ReconcilePeersAsync()
    {
        if (_configuredRuntime != null && _configuredRuntime.TryStart(_serviceToken, StartRuntime))
            SoloLog.Info("p2p fast configured peer enabled", ("peer", _configuredRuntime.Peer));

        var desired = _managedPeers;
        var wanted = new HashSet<string>(desired, StringComparer.OrdinalIgnoreCase);
        foreach (var entry in _managed.Values.ToArray())
        {
            if (wanted.Contains(entry.Peer))
                continue;
            _managed.TryRemove(entry.Peer, out _);
            await StopRuntimeAsync(entry);
        }

        foreach (var peer in desired)
        {
            if (_managed.ContainsKey(peer))
                continue;
            var entry = new PeerRuntime(peer);
            _managed.TryAdd(peer, entry);
            entry.TryStart(_serviceToken, StartRuntime);
        }
    }

    private void StartRuntime(PeerRuntime entry, CancellationToken token)
    {
        entry.ConnectionTask = _client.RunPeerLoopAsync(entry.Peer, token);
        entry.ProbeTask = _autoManaged.ContainsKey(entry.Peer)
            ? Task.CompletedTask
            : RunProbeLoopAsync(entry, token);
    }

    private async Task AutoPeerMaintenanceLoopAsync(CancellationToken ct)
    {
        var initialScan = true;
        while (!ct.IsCancellationRequested)
        {
            if (!initialScan)
            {
                try
                {
                    await _autoRefreshSignal.WaitAsync(AutoDiscoveryInterval, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
            }
            initialScan = false;

            try
            {
                if (Volatile.Read(ref _autoSettings).Enabled)
                    await RefreshAutoPeersAsync(ct);
                else if (!_autoManaged.IsEmpty)
                    await DisableAutoPeersAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _autoDiscoveryError, ex.Message);
                SoloLog.Warn("automatic p2p peer refresh failed", ("error", ex.Message));
            }

        }
    }

    private void RequestAutoRefresh()
    {
        try
        {
            _autoRefreshSignal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private async Task RefreshAutoPeersAsync(CancellationToken ct)
    {
        if (!IsMainnet || !Volatile.Read(ref _autoSettings).Enabled)
            return;

        // Fetch outside the mutation gate so Core RPC latency never blocks peer edits.
        var candidates = await DiscoverMainnetPeersAsync(ct);
        Interlocked.Exchange(ref _lastAutoDiscoveryMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        Volatile.Write(ref _autoDiscoveryError, null);

        await _mutationGate.WaitAsync(ct);
        try
        {
            var settings = Volatile.Read(ref _autoSettings);
            if (!settings.Enabled)
                return;

            if (!settings.EnableIpv6)
                candidates.RemoveAll(candidate => IsIpv6PeerEndpoint(candidate.Peer));
            Volatile.Write(ref _autoCandidateCount, candidates.Count);

            var now = DateTimeOffset.UtcNow;

            var candidateByPeer = candidates.ToDictionary(candidate => candidate.Peer,
                StringComparer.OrdinalIgnoreCase);
            foreach (var entry in _autoManaged.Values)
            {
                if (candidateByPeer.TryGetValue(entry.Peer, out var candidate) && candidate.PingMs.HasValue)
                    entry.SetDiscoveryPing(candidate.PingMs);

                var snapshot = entry.Snapshot(false, false, true, true);
                _autoHistory[entry.Peer] = new PersistedAutoPeerRecord(
                    entry.Peer,
                    snapshot.FastBlockCount,
                    snapshot.LastFastBlockMs,
                    snapshot.AutoSelectedMs);
            }

            if (!settings.EnableIpv6)
            {
                foreach (var entry in _autoManaged.Values.ToArray())
                {
                    if (!IsIpv6PeerEndpoint(entry.Peer) || !_autoManaged.TryRemove(entry.Peer, out _))
                        continue;
                    await StopRuntimeAsync(entry);
                }
            }

            foreach (var entry in _autoManaged.Values.ToArray())
            {
                var snapshot = entry.Snapshot(false, false, true, true);
                if (!HasAutoPeerBecomeInactive(
                        snapshot.LastFastBlockMs,
                        snapshot.AutoSelectedMs,
                        now.ToUnixTimeMilliseconds(),
                        settings.InactiveHours))
                    continue;

                await RemoveAutoPeerAsync(entry);
            }

            // Keep peers that have actually won a fast-job race, then prefer lower
            // Core-reported ping among peers with equal observed fast activity.
            var keep = _autoManaged.Values
                .Select(entry => (Entry: entry, Snapshot: entry.Snapshot(false, false, true, true)))
                .OrderByDescending(item => item.Snapshot.FastBlockCount)
                .ThenByDescending(item => item.Snapshot.LastFastBlockMs)
                .ThenBy(item => item.Snapshot.DiscoveryPingMs ?? double.MaxValue)
                .ThenBy(item => item.Snapshot.AutoSelectedMs)
                .Take(settings.TargetPeers)
                .Select(item => item.Entry.Peer)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var entry in _autoManaged.Values.ToArray())
            {
                if (!keep.Contains(entry.Peer))
                    await RemoveAutoPeerAsync(entry);
            }

            var manual = Volatile.Read(ref _managedPeers);
            foreach (var candidate in OrderAutoCandidates(candidates, _autoHistory))
            {
                if (_autoManaged.Count >= settings.TargetPeers)
                    break;
                if (IsConfiguredPeer(candidate.Peer) ||
                    manual.Contains(candidate.Peer, StringComparer.OrdinalIgnoreCase) ||
                    _autoManaged.ContainsKey(candidate.Peer) ||
                    _blacklistedPeers.ContainsKey(candidate.Peer))
                    continue;

                _autoHistory.TryGetValue(candidate.Peer, out var history);
                if (history != null && HasAutoPeerBecomeInactive(
                        history.LastFastBlockMs,
                        history.AutoSelectedMs,
                        now.ToUnixTimeMilliseconds(),
                        settings.InactiveHours))
                {
                    BlacklistPeer(candidate.Peer);
                    continue;
                }

                var entry = new PeerRuntime(candidate.Peer);
                entry.SetAutoSelected(
                    history?.AutoSelectedMs > 0 ? history.AutoSelectedMs : now.ToUnixTimeMilliseconds(),
                    candidate.PingMs);
                if (history != null)
                    entry.RestoreFastHistory(history.FastBlockCount, history.LastFastBlockMs);
                else
                    _autoHistory[candidate.Peer] = new PersistedAutoPeerRecord(
                        candidate.Peer, 0, 0, now.ToUnixTimeMilliseconds());
                if (!_autoManaged.TryAdd(candidate.Peer, entry))
                {
                    if (history == null)
                        _autoHistory.Remove(candidate.Peer);
                    continue;
                }
                entry.TryStart(_serviceToken, StartRuntime);
            }

            PruneAutoHistory(now);
            await SaveAutoStateAsync(ct);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task DisableAutoPeersAsync(CancellationToken ct)
    {
        await _mutationGate.WaitAsync(ct);
        try
        {
            foreach (var entry in _autoManaged.Values.ToArray())
            {
                var snapshot = entry.Snapshot(false, false, true, true);
                _autoHistory[entry.Peer] = new PersistedAutoPeerRecord(
                    entry.Peer,
                    snapshot.FastBlockCount,
                    snapshot.LastFastBlockMs,
                    snapshot.AutoSelectedMs);
                _autoManaged.TryRemove(entry.Peer, out _);
                await StopRuntimeAsync(entry);
            }
            await SaveAutoStateAsync(ct);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task RemoveAutoPeerAsync(PeerRuntime entry)
    {
        if (!_autoManaged.TryRemove(entry.Peer, out _))
            return;
        BlacklistPeer(entry.Peer);
        await StopRuntimeAsync(entry);
    }

    private async Task PeerRetryTimeoutPrunerLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PeerRetryTimeoutScanInterval, ct);
                await PrunePeersPastRetryLimitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                SoloLog.Warn("p2p fast peer retry-timeout scan failed", ("error", ex.Message));
            }
        }
    }

    private async Task PrunePeersPastRetryLimitAsync(CancellationToken ct)
    {
        await _mutationGate.WaitAsync(ct);
        try
        {
            var now = Stopwatch.GetTimestamp();
            var expiredManual = _managed.Values
                .Where(entry => entry.HasExceededRetryLimit(now, PeerConnectionRetryLimit))
                .ToArray();
            var expiredAuto = _autoManaged.Values
                .Where(entry => entry.HasExceededRetryLimit(now, PeerConnectionRetryLimit))
                .ToArray();
            if (expiredManual.Length == 0 && expiredAuto.Length == 0)
                return;

            foreach (var entry in expiredManual)
                BlacklistPeer(entry.Peer);
            foreach (var entry in expiredAuto)
                BlacklistPeer(entry.Peer);

            // Persist the permanent blacklist before removing peers from their source lists.
            await SaveAutoStateAsync(ct);
            var expiredManualPeers = expiredManual.Select(entry => entry.Peer)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (expiredManualPeers.Count > 0)
            {
                var updated = _managedPeers.Where(peer => !expiredManualPeers.Contains(peer)).ToArray();
                await SavePeersAsync(updated, ct);
                Volatile.Write(ref _managedPeers, updated);
            }

            foreach (var entry in expiredManual)
            {
                _managed.TryRemove(entry.Peer, out _);
                await StopRuntimeAsync(entry);
            }
            foreach (var entry in expiredAuto)
            {
                _autoManaged.TryRemove(entry.Peer, out _);
                await StopRuntimeAsync(entry);
            }

            SoloLog.Warn("p2p fast peers removed after connection retry limit",
                ("manual_peers", expiredManual.Length),
                ("automatic_peers", expiredAuto.Length),
                ("retry_limit_minutes", PeerConnectionRetryLimit.TotalMinutes));
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private void PruneAutoHistory(DateTimeOffset now)
    {
        var retained = new HashSet<string>(_autoManaged.Keys, StringComparer.OrdinalIgnoreCase);
        var retentionCutoff = now.AddDays(-30).ToUnixTimeMilliseconds();
        foreach (var pair in _autoHistory.ToArray())
        {
            if (!retained.Contains(pair.Key) &&
                Math.Max(pair.Value.LastFastBlockMs, pair.Value.AutoSelectedMs) < retentionCutoff)
                _autoHistory.Remove(pair.Key);
        }

        if (_autoHistory.Count <= MaxAutoHistoryRecords)
            return;

        foreach (var key in _autoHistory
                     .Where(pair => !retained.Contains(pair.Key))
                     .OrderBy(pair => Math.Max(pair.Value.LastFastBlockMs, pair.Value.AutoSelectedMs))
                     .Take(_autoHistory.Count - MaxAutoHistoryRecords)
                     .Select(pair => pair.Key)
                     .ToArray())
            _autoHistory.Remove(key);

    }

    internal async Task<List<AutoPeerCandidate>> DiscoverMainnetPeersAsync(CancellationToken ct)
    {
        var peerInfoTask = _rpc.CallAsync<JsonElement>("getpeerinfo", Array.Empty<object>(), ct);
        var nodeAddressesTask = _rpc.CallAsync<JsonElement>(
            "getnodeaddresses", new object[] { AutoAddressQueryCount }, ct);
        await Task.WhenAll(peerInfoTask, nodeAddressesTask);

        return BuildAutoPeerCandidates(peerInfoTask.Result, nodeAddressesTask.Result);
    }

    internal static List<AutoPeerCandidate> BuildAutoPeerCandidates(
        JsonElement peerInfo,
        JsonElement nodeAddresses)
    {
        if (peerInfo.ValueKind != JsonValueKind.Array || nodeAddresses.ValueKind != JsonValueKind.Array)
            throw new ArgumentException("Bitcoin Core peer RPC results must be arrays");

        var candidates = new Dictionary<string, AutoPeerCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (var peer in peerInfo.EnumerateArray())
        {
            if (!peer.TryGetProperty("addr", out var addressElement) ||
                addressElement.ValueKind != JsonValueKind.String ||
                !TryNormalizePublicEndpoint(addressElement.GetString(), port: null, out var endpoint))
                continue;

            double? pingMs = null;
            if (peer.TryGetProperty("pingtime", out var ping) &&
                ping.ValueKind == JsonValueKind.Number && ping.TryGetDouble(out var seconds) &&
                double.IsFinite(seconds) && seconds >= 0)
            {
                var pingMilliseconds = seconds * 1000;
                if (double.IsFinite(pingMilliseconds))
                    pingMs = pingMilliseconds;
            }
            else if (peer.TryGetProperty("pingtime_msec", out ping) &&
                     ping.ValueKind == JsonValueKind.Number && ping.TryGetDouble(out var milliseconds) &&
                     double.IsFinite(milliseconds) && milliseconds >= 0)
                pingMs = milliseconds;

            AddCandidate(candidates, new AutoPeerCandidate(endpoint, pingMs, AddressTimeMs: 0, SourcePriority: 0));
        }

        foreach (var address in nodeAddresses.EnumerateArray())
        {
            if (!address.TryGetProperty("address", out var addressElement) ||
                addressElement.ValueKind != JsonValueKind.String ||
                !address.TryGetProperty("port", out var portElement) ||
                !portElement.TryGetInt32(out var port) ||
                !TryNormalizePublicEndpoint(addressElement.GetString(), port, out var endpoint))
                continue;

            long addressTimeMs = 0;
            if (address.TryGetProperty("time", out var time) && time.TryGetInt64(out var unixTime) && unixTime > 0)
                addressTimeMs = unixTime > long.MaxValue / 1000 ? long.MaxValue : unixTime * 1000;
            AddCandidate(candidates, new AutoPeerCandidate(endpoint, null, addressTimeMs, SourcePriority: 1));
        }

        return candidates.Values
            .OrderBy(candidate => candidate.PingMs.HasValue ? 0 : candidate.SourcePriority + 1)
            .ThenBy(candidate => candidate.PingMs ?? double.MaxValue)
            .ThenByDescending(candidate => candidate.AddressTimeMs)
            .ThenBy(candidate => candidate.Peer, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static void AddCandidate(
        Dictionary<string, AutoPeerCandidate> candidates,
        AutoPeerCandidate candidate)
    {
        if (!candidates.TryGetValue(candidate.Peer, out var current) ||
            candidate.SourcePriority < current.SourcePriority ||
            (candidate.SourcePriority == current.SourcePriority &&
             ((candidate.PingMs.HasValue && (!current.PingMs.HasValue || candidate.PingMs < current.PingMs)) ||
              candidate.AddressTimeMs > current.AddressTimeMs)))
            candidates[candidate.Peer] = candidate;
    }

    private static IEnumerable<AutoPeerCandidate> OrderAutoCandidates(
        IEnumerable<AutoPeerCandidate> candidates,
        IReadOnlyDictionary<string, PersistedAutoPeerRecord> history) =>
        candidates
            .OrderBy(candidate => history.TryGetValue(candidate.Peer, out var record) && record.FastBlockCount > 0
                ? 0
                : 1)
            .ThenByDescending(candidate => history.TryGetValue(candidate.Peer, out var record)
                ? record.FastBlockCount
                : 0)
            .ThenByDescending(candidate => history.TryGetValue(candidate.Peer, out var record)
                ? record.LastFastBlockMs
                : 0)
            .ThenBy(candidate => candidate.PingMs.HasValue ? 0 : candidate.SourcePriority + 1)
            .ThenBy(candidate => candidate.PingMs ?? double.MaxValue)
            .ThenByDescending(candidate => candidate.AddressTimeMs)
            .ThenBy(candidate => candidate.Peer, StringComparer.OrdinalIgnoreCase);

    internal static bool TryNormalizePublicEndpoint(string? address, int? port, out string endpoint)
    {
        endpoint = string.Empty;
        if (string.IsNullOrWhiteSpace(address))
            return false;

        string host;
        int endpointPort;
        if (port.HasValue)
        {
            host = address.StartsWith("[", StringComparison.Ordinal) && address.EndsWith("]", StringComparison.Ordinal)
                ? address[1..^1]
                : address;
            endpointPort = port.Value;
        }
        else
        {
            try
            {
                var parsed = ParseEndpoint(address, "mainnet", allowDefaultPort: false);
                host = parsed.Host;
                endpointPort = parsed.Port;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        if (endpointPort is < 1 or > 65535 || !IPAddress.TryParse(host, out var ip))
            return false;
        if (ip.IsIPv4MappedToIPv6)
            ip = ip.MapToIPv4();
        if (!IsPublicAddress(ip))
            return false;

        var formattedHost = ip.AddressFamily == AddressFamily.InterNetworkV6 ? $"[{ip}]" : ip.ToString();
        try
        {
            endpoint = NormalizeManagedPeer($"{formattedHost}:{endpointPort}", "mainnet");
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool IsPublicAddress(IPAddress address)
    {
        var bytes = address.GetAddressBytes();
        if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            // Global unicast IPv6 is currently allocated from 2000::/3.
            if ((bytes[0] & 0xe0) != 0x20)
                return false;
            return !(bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0x0d && bytes[3] == 0xb8);
        }

        var first = bytes[0];
        var second = bytes[1];
        return first != 0 && first != 10 && first != 127 && first < 224 &&
               !(first == 100 && second is >= 64 and <= 127) &&
               !(first == 169 && second == 254) &&
               !(first == 172 && second is >= 16 and <= 31) &&
               !(first == 192 && second == 168) &&
               !(first == 192 && second == 0 && bytes[2] is 0 or 2) &&
               !(first == 198 && second is 18 or 19) &&
               !(first == 198 && second == 51 && bytes[2] == 100) &&
               !(first == 203 && second == 0 && bytes[2] == 113);
    }

    private async Task SaveAutoStateAsync(CancellationToken ct)
    {
        var state = new PersistedAutoPeerState(
            Volatile.Read(ref _autoSettings),
            _blacklistedPeers.Keys.OrderBy(peer => peer, StringComparer.OrdinalIgnoreCase).ToArray(),
            _autoHistory.Values.ToArray());
        var temp = _autoStateFilePath + ".tmp";
        var json = JsonSerializer.Serialize(state, AppConfig.JsonOptions());
        try
        {
            await File.WriteAllTextAsync(temp, json, ct);
            File.Move(temp, _autoStateFilePath, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp))
                File.Delete(temp);
        }
    }

    private static void ValidateAutoSettings(P2pFastAutoSettings settings)
    {
        if (settings.TargetPeers is < 1 or > MaxAutoPeers)
            throw new ArgumentOutOfRangeException(nameof(settings), $"Automatic peer target must be 1..{MaxAutoPeers}");
        if (settings.InactiveHours is < 1 or > 168)
            throw new ArgumentOutOfRangeException(nameof(settings), "Peer inactivity timeout must be 1..168 hours");
    }

    private static bool IsUnixMilliseconds(long value) =>
        value >= 0 && value <= DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();

    internal static bool HasAutoPeerBecomeInactive(
        long lastFastBlockMs,
        long autoSelectedMs,
        long nowMs,
        int inactiveHours)
    {
        var lastActivityMs = lastFastBlockMs > 0 ? lastFastBlockMs : autoSelectedMs;
        if (lastActivityMs <= 0 || !IsUnixMilliseconds(lastActivityMs) || !IsUnixMilliseconds(nowMs))
            return true;
        if (lastActivityMs > nowMs)
            return false;
        return nowMs - lastActivityMs > TimeSpan.FromHours(inactiveHours).TotalMilliseconds;
    }

    internal static bool HasPeerRetryLimitElapsed(
        long retryStartedTimestamp,
        long nowTimestamp,
        TimeSpan retryLimit)
    {
        if (retryStartedTimestamp <= 0 || nowTimestamp < retryStartedTimestamp || retryLimit <= TimeSpan.Zero)
            return false;
        return Stopwatch.GetElapsedTime(retryStartedTimestamp, nowTimestamp) >= retryLimit;
    }

    internal static bool HasPeerConnectionBeenStable(
        long connectedStartedTimestamp,
        long disconnectedTimestamp,
        TimeSpan stableDuration)
    {
        if (connectedStartedTimestamp <= 0 || disconnectedTimestamp < connectedStartedTimestamp ||
            stableDuration <= TimeSpan.Zero)
            return false;
        return Stopwatch.GetElapsedTime(connectedStartedTimestamp, disconnectedTimestamp) >= stableDuration;
    }

    internal static long GetRetryStartedTimestampAfterDisconnect(
        long retryStartedTimestamp,
        long connectedStartedTimestamp,
        long disconnectedTimestamp,
        TimeSpan stableDuration)
    {
        if (HasPeerConnectionBeenStable(connectedStartedTimestamp, disconnectedTimestamp, stableDuration))
            return 0;
        if (retryStartedTimestamp > 0)
            return retryStartedTimestamp;
        return connectedStartedTimestamp > 0 ? connectedStartedTimestamp : disconnectedTimestamp;
    }

    internal static bool IsIpv6PeerEndpoint(string peer) =>
        peer.StartsWith("[", StringComparison.Ordinal);

    private void BlacklistPeer(string peer)
    {
        _blacklistedPeers.TryAdd(peer, 0);
        _autoHistory.Remove(peer);
    }

    private bool IsMainnet => _cfg.NetworkName.Equals("mainnet", StringComparison.OrdinalIgnoreCase) ||
                              _cfg.NetworkName.Equals("bitcoin", StringComparison.OrdinalIgnoreCase);

    private async Task RunProbeLoopAsync(PeerRuntime entry, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await ProbeAverageAsync(entry.Peer, 3, ct);
                entry.SetPing(result.AverageMs, result.SuccessfulSamples, null);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                entry.SetPing(null, 0, ex.Message);
            }

            try
            {
                await Task.Delay(ProbeInterval, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private async Task<ProbeResult> ProbeAverageAsync(string peer, int attempts, CancellationToken ct)
    {
        var (host, port) = ParseEndpoint(peer, _cfg.NetworkName, allowDefaultPort: false);
        var samples = new List<double>(attempts);
        string? lastError = null;
        for (var i = 0; i < attempts; i++)
        {
            using var client = new TcpClient();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ProbeTimeout);
            var timer = Stopwatch.StartNew();
            try
            {
                await client.ConnectAsync(host, port, timeout.Token);
                timer.Stop();
                samples.Add(timer.Elapsed.TotalMilliseconds);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                timer.Stop();
                lastError = ex is OperationCanceledException
                    ? $"TCP connect timed out after {ProbeTimeout.TotalSeconds:0} seconds"
                    : ex.Message;
            }
        }

        if (samples.Count == 0)
            throw new IOException(lastError ?? "TCP connect failed");
        return new ProbeResult(samples.Average(), samples.Count);
    }

    private async Task SavePeersAsync(string[] peers, CancellationToken ct)
    {
        var temp = _peerFilePath + ".tmp";
        var json = JsonSerializer.Serialize(peers, AppConfig.JsonOptions());
        try
        {
            await File.WriteAllTextAsync(temp, json, ct);
            File.Move(temp, _peerFilePath, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp))
                File.Delete(temp);
        }
    }

    private PeerRuntime? FindEntry(string peer)
    {
        if (_managed.TryGetValue(peer, out var managed))
            return managed;
        if (_autoManaged.TryGetValue(peer, out var automatic))
            return automatic;
        return _configuredPeer != null && peer.Equals(_configuredPeer, StringComparison.OrdinalIgnoreCase)
            ? _configuredRuntime
            : null;
    }

    private void OnPeerStateChanged(string peer, string state, string? error) =>
        FindEntry(peer)?.SetState(state, error);

    private void OnPeerTcpConnected(string peer, double latencyMs) =>
        FindEntry(peer)?.RecordTcpConnected(latencyMs);

    private void OnFastBlockPublished(string peer) => FindEntry(peer)?.RecordFastBlock();

    private bool IsConfiguredPeer(string peer) =>
        _configuredPeer != null && peer.Equals(_configuredPeer, StringComparison.OrdinalIgnoreCase);

    private void EnsureNotConfiguredPeer(string peer)
    {
        if (IsConfiguredPeer(peer))
            throw new InvalidOperationException("Peer is already configured as bitcoind.p2p_fast_peer");
    }

    private void EnsureManagementEnabled()
    {
        if (!_managementEnabled)
            throw new InvalidOperationException("P2P-fast peer management is disabled");
    }

    private async Task WaitUntilReadyAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _started) == 0)
            throw new InvalidOperationException("P2P-fast peer manager is not running");
        await _ready.Task.WaitAsync(ct);
    }

    private async Task StopAllAsync()
    {
        foreach (var entry in _managed.Values.ToArray())
            await StopRuntimeAsync(entry);
        _managed.Clear();
        foreach (var entry in _autoManaged.Values.ToArray())
            await StopRuntimeAsync(entry);
        _autoManaged.Clear();
        if (_configuredRuntime != null)
            await StopRuntimeAsync(_configuredRuntime);
    }

    private static async Task StopRuntimeAsync(PeerRuntime entry) => await entry.StopAsync();

    private static string NormalizeConfiguredPeer(string peer, string network)
    {
        if (string.IsNullOrWhiteSpace(peer))
            throw new ArgumentException("Peer is empty", nameof(peer));
        var endpoint = ParseEndpoint(peer, network, allowDefaultPort: true);
        var host = endpoint.Host.Contains(':') ? $"[{endpoint.Host}]" : endpoint.Host.ToLowerInvariant();
        return $"{host}:{endpoint.Port}";
    }

    private static int DefaultPort(string network) => network.ToLowerInvariant() switch
    {
        "mainnet" or "bitcoin" => 8333,
        "testnet" => 18333,
        "regtest" => 18444,
        _ => throw new InvalidOperationException($"unsupported P2P network: {network}")
    };

    private static bool IsValidHost(string host)
    {
        if (string.IsNullOrWhiteSpace(host) || host.Length > 253 || host.Any(char.IsWhiteSpace))
            return false;
        if (IPAddress.TryParse(host, out _))
            return true;
        return Regex.IsMatch(host, @"^[a-zA-Z0-9_](?:[a-zA-Z0-9_.-]*[a-zA-Z0-9_-])?$");
    }

    private readonly record struct ProbeResult(double AverageMs, int SuccessfulSamples);

    private sealed class PeerRuntime
    {
        private readonly object _gate = new();
        private readonly bool _exemptFromRetryLimit;
        private CancellationTokenSource? _cts;
        private string _state = "connecting";
        private string? _lastError;
        private string? _probeError;
        private double? _tcpPingMs;
        private int _pingSamples;
        private long _fastBlockCount;
        private long _connectionCount;
        private long _reconnectCount;
        private long _firstConnectedMs;
        private long _connectedSinceMs;
        private long _connectedSinceTimestamp;
        private long _lastConnectedMs;
        private long _lastFastBlockMs;
        private long _autoSelectedMs;
        private double? _discoveryPingMs;
        private long _retryStartedTimestamp;

        public PeerRuntime(string peer, bool exemptFromRetryLimit = false)
        {
            Peer = peer;
            _exemptFromRetryLimit = exemptFromRetryLimit;
        }

        public string Peer { get; }
        public Task? ConnectionTask { get; set; }
        public Task? ProbeTask { get; set; }

        public bool HasExceededRetryLimit(long nowTimestamp, TimeSpan retryLimit)
        {
            if (_exemptFromRetryLimit)
                return false;
            lock (_gate)
                return _state != "connected" &&
                       HasPeerRetryLimitElapsed(_retryStartedTimestamp, nowTimestamp, retryLimit);
        }

        public bool TryStart(CancellationToken serviceToken, Action<PeerRuntime, CancellationToken> start)
        {
            lock (_gate)
            {
                if (_cts is { IsCancellationRequested: false })
                    return false;
                _cts?.Dispose();
                _cts = CancellationTokenSource.CreateLinkedTokenSource(serviceToken);
                start(this, _cts.Token);
                return true;
            }
        }

        public async Task StopAsync()
        {
            Task? connection;
            Task? probe;
            CancellationTokenSource? cts;
            lock (_gate)
            {
                cts = _cts;
                if (cts == null)
                    return;
                cts.Cancel();
                connection = ConnectionTask;
                probe = ProbeTask;
                _cts = null;
                ConnectionTask = null;
                ProbeTask = null;
            }

            try
            {
                await Task.WhenAll(new[] { connection, probe }.OfType<Task>());
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { SoloLog.Debug("p2p peer stopped", ("peer", Peer), ("error", ex.Message)); }
            finally { cts.Dispose(); }
        }

        public void EnsureProbeRunning(Func<CancellationToken, Task> start)
        {
            lock (_gate)
            {
                if (_cts is not { IsCancellationRequested: false } cts ||
                    ProbeTask is { IsCompleted: false })
                    return;
                ProbeTask = start(cts.Token);
            }
        }

        public void SetState(string state, string? error)
        {
            lock (_gate)
            {
                var nowTimestamp = Stopwatch.GetTimestamp();
                if (state == "connecting" && _retryStartedTimestamp == 0)
                    _retryStartedTimestamp = nowTimestamp;
                else if (state == "failed" && _state == "connected")
                {
                    _retryStartedTimestamp = GetRetryStartedTimestampAfterDisconnect(
                        _retryStartedTimestamp,
                        _connectedSinceTimestamp,
                        nowTimestamp,
                        PeerConnectionRetryLimit);
                }

                if (state == "connecting" && _state is ("failed" or "reconnecting" or "connected"))
                    _reconnectCount++;
                _state = state;
                if (error != null)
                    _lastError = error;
                else if (state == "connected")
                    _lastError = null;

                if (state == "connected")
                {
                    _connectedSinceTimestamp = nowTimestamp;
                    var now = UtcNowMs();
                    _connectedSinceMs = now;
                    _lastConnectedMs = now;
                    if (_firstConnectedMs == 0)
                        _firstConnectedMs = now;
                }
                else if (state is "connecting" or "failed" or "reconnecting" or "handshaking")
                {
                    _connectedSinceMs = 0;
                    if (state == "failed")
                        _connectedSinceTimestamp = 0;
                }
            }
        }

        public void SetPing(double? latencyMs, int samples, string? error)
        {
            lock (_gate)
            {
                _tcpPingMs = latencyMs.HasValue ? Math.Round(latencyMs.Value, 2) : null;
                _pingSamples = samples;
                _probeError = error;
            }
        }

        public void RecordTcpConnected(double latencyMs)
        {
            lock (_gate)
            {
                _connectionCount++;
                var now = UtcNowMs();
                _lastConnectedMs = now;
                _discoveryPingMs = Math.Round(latencyMs, 2);
                if (_pingSamples == 0 || !_tcpPingMs.HasValue)
                    _tcpPingMs = Math.Round(latencyMs, 2);
            }
        }

        public void RecordFastBlock()
        {
            Interlocked.Increment(ref _fastBlockCount);
            Interlocked.Exchange(ref _lastFastBlockMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }

        public void RestoreFastHistory(long count, long lastFastBlockMs)
        {
            Interlocked.Exchange(ref _fastBlockCount, count);
            Interlocked.Exchange(ref _lastFastBlockMs, lastFastBlockMs);
        }

        public void SetAutoSelected(long selectedMs, double? discoveryPingMs)
        {
            lock (_gate)
            {
                _autoSelectedMs = selectedMs;
                _discoveryPingMs = discoveryPingMs;
            }
        }

        public void SetDiscoveryPing(double? discoveryPingMs)
        {
            if (!discoveryPingMs.HasValue)
                return;
            lock (_gate)
                _discoveryPingMs = discoveryPingMs;
        }

        public P2pFastPeerSnapshot Snapshot(bool isManaged, bool isConfigured, bool isActive, bool isAuto = false)
        {
            lock (_gate)
            {
                return new P2pFastPeerSnapshot(
                    Peer,
                    isManaged,
                    isConfigured,
                    isAuto,
                    isActive,
                    _state,
                    _tcpPingMs,
                    _pingSamples,
                    Interlocked.Read(ref _fastBlockCount),
                    _connectionCount,
                    _reconnectCount,
                    _firstConnectedMs,
                    _connectedSinceMs,
                    _lastConnectedMs,
                    _lastError,
                    _probeError,
                    Interlocked.Read(ref _lastFastBlockMs),
                    _autoSelectedMs,
                    _discoveryPingMs);
            }
        }

        private static long UtcNowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}

public sealed record P2pFastPeerSnapshot(
    string Peer,
    bool IsManaged,
    bool IsConfigured,
    bool IsAuto,
    bool IsActive,
    string State,
    double? TcpPingMs,
    int PingSamples,
    long FastBlockCount,
    long ConnectionCount,
    long ReconnectCount,
    long FirstConnectedMs,
    long ConnectedSinceMs,
    long LastConnectedMs,
    string? LastError,
    string? ProbeError,
    long LastFastBlockMs,
    long AutoSelectedMs,
    double? DiscoveryPingMs)
{
    public int FastRank { get; set; }
}

public sealed record P2pFastAutoSettings(
    bool Enabled,
    int TargetPeers,
    int InactiveHours,
    bool EnableIpv6 = false)
{
    public static P2pFastAutoSettings Default { get; } = new(true, 30, 24, false);
}

public sealed record P2pFastAutoStatus(
    P2pFastAutoSettings Settings,
    bool Available,
    int ActivePeers,
    int BlacklistedPeers,
    string[] BlacklistedPeerSample,
    int CandidatePeers,
    long LastDiscoveryMs,
    string? LastError);

internal sealed record PersistedAutoPeerState(
    P2pFastAutoSettings? Settings,
    string[]? BlacklistedPeers,
    PersistedAutoPeerRecord[]? PeerHistory)
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public PersistedAutoCooldown[]? Cooldowns { get; init; }
}

internal sealed record PersistedAutoCooldown(string Peer, string RetryAfterUtc);

internal sealed record PersistedAutoPeerRecord(
    string Peer,
    long FastBlockCount,
    long LastFastBlockMs,
    long AutoSelectedMs);

internal readonly record struct AutoPeerCandidate(
    string Peer,
    double? PingMs,
    long AddressTimeMs,
    int SourcePriority);
