using System.Diagnostics;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.RegularExpressions;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.P2p;

public sealed class P2pFastPeerManager
{
    private const int MaxPeers = 128;
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromMinutes(30);
    private readonly AppConfig _cfg;
    private readonly P2pFastPeer _client;
    private readonly string _peerFilePath;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly ConcurrentDictionary<string, PeerRuntime> _managed = new(StringComparer.OrdinalIgnoreCase);
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly PeerRuntime? _configuredRuntime;
    private readonly string? _configuredPeer;
    private string[] _managedPeers = Array.Empty<string>();
    private CancellationToken _serviceToken;
    private int _started;
    private readonly bool _managementEnabled;
    private readonly bool _enabled;

    public P2pFastPeerManager(AppConfig cfg, TemplateEngine engine, BitcoinRpcClient rpc)
    {
        _cfg = cfg;
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
            _configuredRuntime = new PeerRuntime(_configuredPeer);

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
        try
        {
            await InitializeAsync(ct);
            _ready.TrySetResult();
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
            await StopAllAsync();
        }
    }

    public async Task<IReadOnlyList<P2pFastPeerSnapshot>> GetPeersAsync(CancellationToken ct = default)
    {
        await WaitUntilReadyAsync(ct);
        var result = _managed.Values.Select(entry => entry.Snapshot(
            isManaged: true,
            isConfigured: false,
            isActive: true)).ToList();

        if (_configuredRuntime != null)
            result.Add(_configuredRuntime.Snapshot(
                isManaged: false,
                isConfigured: true,
                isActive: true));

        return result.OrderBy(peer => peer.Peer, StringComparer.OrdinalIgnoreCase).ToArray();
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

            var updated = current.Append(normalized).ToArray();
            await SavePeersAsync(updated, ct);
            Volatile.Write(ref _managedPeers, updated);
            await ReconcilePeersAsync();
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
            if (!current.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                return false;

            var updated = current.Where(value =>
                !value.Equals(normalized, StringComparison.OrdinalIgnoreCase)).ToArray();
            await SavePeersAsync(updated, ct);
            Volatile.Write(ref _managedPeers, updated);
            await ReconcilePeersAsync();
            return true;
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    public async Task ReplacePeersAsync(IReadOnlyList<string> peers, CancellationToken ct = default)
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
            EnsureNotConfiguredPeer(value);
            if (!seen.Add(value))
                throw new ArgumentException($"Duplicate peer: {value}", nameof(peers));
            normalized.Add(value);
        }

        await WaitUntilReadyAsync(ct);
        await _mutationGate.WaitAsync(ct);
        try
        {
            var updated = normalized.ToArray();
            await SavePeersAsync(updated, ct);
            Volatile.Write(ref _managedPeers, updated);
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
        entry.ProbeTask = RunProbeLoopAsync(entry, token);
    }

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

    private sealed class PeerRuntime(string peer)
    {
        private readonly object _gate = new();
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
        private long _lastConnectedMs;

        public string Peer { get; } = peer;
        public Task? ConnectionTask { get; set; }
        public Task? ProbeTask { get; set; }

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

        public void SetState(string state, string? error)
        {
            lock (_gate)
            {
                if (state == "connecting" && _state is ("failed" or "reconnecting" or "connected"))
                    _reconnectCount++;
                _state = state;
                if (error != null)
                    _lastError = error;
                else if (state == "connected")
                    _lastError = null;

                if (state == "connected")
                {
                    var now = UtcNowMs();
                    _connectedSinceMs = now;
                    _lastConnectedMs = now;
                    if (_firstConnectedMs == 0)
                        _firstConnectedMs = now;
                }
                else if (state is "connecting" or "failed" or "reconnecting" or "handshaking")
                {
                    _connectedSinceMs = 0;
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
                if (!_tcpPingMs.HasValue)
                    _tcpPingMs = Math.Round(latencyMs, 2);
            }
        }

        public void RecordFastBlock() => Interlocked.Increment(ref _fastBlockCount);

        public P2pFastPeerSnapshot Snapshot(bool isManaged, bool isConfigured, bool isActive)
        {
            lock (_gate)
            {
                return new P2pFastPeerSnapshot(
                    Peer,
                    isManaged,
                    isConfigured,
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
                    _probeError);
            }
        }

        private static long UtcNowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}

public sealed record P2pFastPeerSnapshot(
    string Peer,
    bool IsManaged,
    bool IsConfigured,
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
    string? ProbeError);
