using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.P2p;

/// <summary>Bounded address-fetch connections, independent of fast block delivery and Core RPC.</summary>
internal sealed class P2pAddressDiscovery
{
    private const int MaxCandidates = 4096;
    private const int MaxAddressesPerMessage = 1000;
    private static readonly string[] DnsSeeds =
    [
        "seed.bitcoin.sipa.be", "dnsseed.bluematt.me", "seed.bitcoin.jonasschnelli.ch",
        "seed.btc.petertodd.net", "seed.bitcoin.sprovoost.nl", "dnsseed.emzy.de",
        "seed.bitcoin.wiz.biz", "seed.mainnet.achownodes.xyz"
    ];
    private readonly string _cachePath;
    private readonly string _network;
    private readonly uint _magic;
    private readonly Func<string, CancellationToken, Task<IPAddress[]>> _resolve;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<string, AutoPeerCandidate> _candidates = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _nextFetch = new(StringComparer.OrdinalIgnoreCase);
    private string[] _dnsBootstrap = [];
    private long _nextDnsLookup;
    private bool _loaded;

    internal P2pAddressDiscovery(
        string dataDir, string network,
        Func<string, CancellationToken, Task<IPAddress[]>>? resolve = null)
    {
        _cachePath = Path.Combine(dataDir, "p2p-fast-addresses.json");
        _network = network.ToLowerInvariant();
        _magic = network.ToLowerInvariant() switch
        {
            "mainnet" or "bitcoin" => 0xd9b4bef9,
            "regtest" => 0xdab5bffa,
            "testnet" => 0x0709110b,
            _ => throw new ArgumentException("Unsupported discovery network", nameof(network))
        };
        _resolve = resolve ?? ((host, ct) => Dns.GetHostAddressesAsync(host, ct));
    }

    internal async Task<List<AutoPeerCandidate>> DiscoverAsync(
        IEnumerable<string> bootstrap, bool enableIpv6, Func<string, bool> isBlacklisted,
        CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await LoadAsync(ct);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var candidate in _candidates.Values.ToArray())
                if (candidate.AddressTimeMs < now - TimeSpan.FromDays(30).TotalMilliseconds)
                    _candidates.Remove(candidate.Peer);
            foreach (var source in _nextFetch.Where(pair => pair.Value < now - 86_400_000).Select(pair => pair.Key).ToArray())
                _nextFetch.Remove(source);

            bool Eligible(string peer) => !isBlacklisted(peer) &&
                (enableIpv6 || !P2pFastPeerManager.IsIpv6PeerEndpoint(peer)) &&
                (!_nextFetch.TryGetValue(peer, out var next) || next <= now);

            // Rotate through unqueried addresses. Persistent/mining connections are
            // not replaced merely because a new discovery round starts.
            var known = bootstrap.Distinct(StringComparer.OrdinalIgnoreCase).Where(Eligible).ToArray();
            var independent = _candidates.Keys.Except(known, StringComparer.OrdinalIgnoreCase)
                .Where(Eligible).OrderBy(_ => Random.Shared.Next());
            // Reserve half the queries for learned addresses rather than repeatedly
            // asking only the current mining peers for the same directory.
            var sources = known.Take(2).Concat(independent.Take(2)).Concat(known.Skip(2))
                .Take(4).ToArray();
            var responded = await FetchSourcesAsync(sources, now, ct);
            if (!responded && (_network is "mainnet" or "bitcoin"))
            {
                await ResolveBootstrapAsync(now, ct);
                sources = _dnsBootstrap.Where(Eligible).OrderBy(_ => Random.Shared.Next()).Take(4).ToArray();
                await FetchSourcesAsync(sources, now, ct);
            }

            var retained = _candidates.Values.OrderByDescending(candidate => candidate.AddressTimeMs)
                .Take(MaxCandidates).ToArray();
            _candidates.Clear();
            foreach (var candidate in retained)
                _candidates[candidate.Peer] = candidate;
            await SaveAsync(retained, ct);
            return retained.Where(candidate => !isBlacklisted(candidate.Peer)).ToList();
        }
        finally { _gate.Release(); }
    }

    private async Task<bool> FetchSourcesAsync(string[] sources, long now, CancellationToken ct)
    {
        var responded = false;
        // At most two short discovery sockets at once, with one getaddr per session.
        foreach (var batch in sources.Chunk(2))
        {
            var results = await Task.WhenAll(batch.Select(source => FetchAsync(source, ct)));
            for (var i = 0; i < results.Length; i++)
            {
                var result = results[i];
                responded |= result.Count > 0;
                _nextFetch[batch[i]] = now + (result.Count > 0 ? 30 : 5) * 60_000;
                foreach (var candidate in result)
                    if (!_candidates.TryGetValue(candidate.Peer, out var old) || old.AddressTimeMs < candidate.AddressTimeMs)
                        _candidates[candidate.Peer] = candidate;
            }
        }
        return responded;
    }

    private async Task ResolveBootstrapAsync(long now, CancellationToken ct)
    {
        if (now < _nextDnsLookup)
            return;
        _nextDnsLookup = now + 30 * 60_000;
        var results = await Task.WhenAll(DnsSeeds.OrderBy(_ => Random.Shared.Next()).Take(3).Select(async seed =>
        {
            foreach (var host in new[] { "x9." + seed, seed })
            {
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    timeout.CancelAfter(TimeSpan.FromSeconds(3));
                    var ips = await _resolve(host, timeout.Token);
                    var peers = ips.Select(ip => P2pFastPeerManager.TryNormalizePublicEndpoint(ip.ToString(), 8333, out var peer)
                            ? peer : null).OfType<string>().Distinct().Take(32).ToArray();
                    if (peers.Length > 0)
                        return peers;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                catch (Exception ex) when (ex is SocketException or OperationCanceledException)
                {
                    SoloLog.Debug("p2p discovery seed unavailable", ("seed", host), ("error", ex.Message));
                }
            }
            return Array.Empty<string>();
        }));
        var resolved = results.SelectMany(peers => peers).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        if (resolved.Length > 0)
            _dnsBootstrap = resolved;
    }

    private async Task<List<AutoPeerCandidate>> FetchAsync(string source, CancellationToken ct)
    {
        var learned = new List<AutoPeerCandidate>();
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(8));
            var token = timeout.Token;
            var (host, port) = P2pFastPeerManager.ParseEndpoint(source, _network, allowDefaultPort: false);
            using var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(host, port, token);
            await using var stream = client.GetStream();
            var remote = (IPEndPoint)client.Client.RemoteEndPoint!;
            var version = P2pFastPeer.BuildVersionPayload(remote, startHeight: 0);
            // Address-fetch connections do not provide full blocks or transactions.
            BinaryPrimitives.WriteUInt64LittleEndian(version.AsSpan(4, 8), 0);
            await SendAsync(stream, "version", version, token);
            var gotVersion = false;
            var requested = false;
            for (var messages = 0; messages < 64; messages++)
            {
                var message = await ReadAsync(stream, token);
                switch (message.Command)
                {
                    case "version" when !gotVersion:
                        if (message.Payload.Length < 80)
                            throw new InvalidDataException("truncated discovery version");
                        var protocol = BinaryPrimitives.ReadInt32LittleEndian(message.Payload);
                        if (protocol < 70001)
                            throw new InvalidDataException("unsupported discovery protocol version");
                        gotVersion = true;
                        if (protocol >= 70016)
                            await SendAsync(stream, "sendaddrv2", [], token);
                        await SendAsync(stream, "verack", [], token);
                        break;
                    case "verack" when gotVersion && !requested:
                        requested = true;
                        await SendAsync(stream, "getaddr", [], token);
                        break;
                    case "ping" when message.Payload.Length == 8:
                        await SendAsync(stream, "pong", message.Payload, token);
                        break;
                    case "addr" or "addrv2" when requested:
                        if (!TryParseAddresses(message.Command, message.Payload, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), out var parsed))
                            throw new InvalidDataException("malformed discovery address message");
                        learned.AddRange(parsed);
                        // A peer may send a one-address self announcement before its
                        // getaddr response. Wait for the response or the session deadline.
                        var offset = 0;
                        TryReadCompactSize(message.Payload, ref offset, out var count);
                        if (count != 1 || learned.Count >= MaxAddressesPerMessage)
                            return learned.Take(MaxAddressesPerMessage).ToList();
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception ex) when (ex is IOException or InvalidDataException or SocketException or OperationCanceledException or ArgumentException)
        {
            SoloLog.Debug("p2p address fetch ended", ("peer", source), ("addresses", learned.Count), ("error", ex.Message));
        }
        return learned.Take(MaxAddressesPerMessage).ToList();
    }

    internal static bool TryParseAddresses(
        string command, ReadOnlySpan<byte> payload, long nowMs, out AutoPeerCandidate[] candidates)
    {
        candidates = [];
        if (command is not ("addr" or "addrv2"))
            return false;
        var pos = 0;
        if (!TryReadCompactSize(payload, ref pos, out var count) || count > MaxAddressesPerMessage)
            return false;
        var parsed = new Dictionary<string, AutoPeerCandidate>(StringComparer.OrdinalIgnoreCase);
        for (ulong i = 0; i < count; i++)
        {
            if (payload.Length - pos < 4)
                return false;
            var timeMs = (long)BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(pos, 4)) * 1000;
            pos += 4;
            ulong services;
            ReadOnlySpan<byte> address;
            if (command == "addrv2")
            {
                if (!TryReadCompactSize(payload, ref pos, out services) || pos >= payload.Length)
                    return false;
                var network = payload[pos++];
                if (!TryReadCompactSize(payload, ref pos, out var length) || length > 512 || payload.Length - pos < (int)length + 2)
                    return false;
                address = payload.Slice(pos, (int)length);
                pos += (int)length;
                if ((network == 1 && length != 4) || (network == 2 && length != 16))
                    return false;
                if (network is not (1 or 2) || (network == 2 && new IPAddress(address).IsIPv4MappedToIPv6))
                    address = [];
            }
            else
            {
                if (payload.Length - pos < 26)
                    return false;
                services = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(pos, 8));
                address = payload.Slice(pos + 8, 16);
                pos += 24;
            }
            var port = BinaryPrimitives.ReadUInt16BigEndian(payload.Slice(pos, 2));
            pos += 2;
            // Full or pruned block service, with witness support. Unsupported networks
            // are skipped; no Tor/I2P/CJDNS address is interpreted as an IP endpoint.
            if (address.IsEmpty || (services & (1UL | 1024UL)) == 0 || (services & 8UL) == 0)
                continue;
            if (!P2pFastPeerManager.TryNormalizePublicEndpoint(new IPAddress(address).ToString(), port, out var peer))
                continue;
            if (timeMs == 0 || timeMs > nowMs + 600_000)
                timeMs = nowMs - 5 * 86_400_000L;
            if (timeMs < nowMs - 30 * 86_400_000L)
                continue;
            var candidate = new AutoPeerCandidate(peer, null, timeMs, SourcePriority: 1);
            if (!parsed.TryGetValue(peer, out var old) || old.AddressTimeMs < timeMs)
                parsed[peer] = candidate;
        }
        if (pos != payload.Length)
            return false;
        candidates = parsed.Values.ToArray();
        return true;
    }

    private static bool TryReadCompactSize(ReadOnlySpan<byte> bytes, ref int pos, out ulong value)
    {
        value = 0;
        if (pos >= bytes.Length)
            return false;
        var prefix = bytes[pos++];
        if (prefix < 253) { value = prefix; return true; }
        var size = prefix == 253 ? 2 : prefix == 254 ? 4 : 8;
        if (bytes.Length - pos < size)
            return false;
        value = size == 2 ? BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(pos, size)) :
            size == 4 ? BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(pos, size)) :
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(pos, size));
        pos += size;
        return size == 2 ? value >= 253 : size == 4 ? value > ushort.MaxValue : value > uint.MaxValue;
    }

    private async Task SendAsync(NetworkStream stream, string command, byte[] payload, CancellationToken ct)
    {
        var frame = new byte[24 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), _magic);
        Encoding.ASCII.GetBytes(command.AsSpan(), frame.AsSpan(4, 12));
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(16, 4), (uint)payload.Length);
        BitcoinEncoding.DoubleSha256(payload).AsSpan(0, 4).CopyTo(frame.AsSpan(20, 4));
        payload.CopyTo(frame, 24);
        await stream.WriteAsync(frame, ct);
    }

    private async Task<(string Command, byte[] Payload)> ReadAsync(NetworkStream stream, CancellationToken ct)
    {
        var header = new byte[24];
        await stream.ReadExactlyAsync(header, ct);
        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != _magic)
            throw new InvalidDataException("discovery peer network magic mismatch");
        var command = Encoding.ASCII.GetString(header, 4, 12).TrimEnd('\0');
        var length = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        if (length > 4 * 1024 * 1024 ||
            ((command is "addr" or "addrv2") && length > 600 * 1024) ||
            (command == "version" && length > 1024) ||
            (command == "verack" && length != 0) || (command == "ping" && length != 8))
            throw new InvalidDataException("oversized discovery message");
        if (command is "version" or "verack" or "addr" or "addrv2" or "ping")
        {
            var payload = new byte[(int)length];
            await stream.ReadExactlyAsync(payload, ct);
            if (!BitcoinEncoding.DoubleSha256(payload).AsSpan(0, 4).SequenceEqual(header.AsSpan(20, 4)))
                throw new InvalidDataException("discovery message checksum mismatch");
            return (command, payload);
        }
        var scratch = ArrayPool<byte>.Shared.Rent(8192);
        try
        {
            for (var remaining = (int)length; remaining > 0;)
            {
                var count = await stream.ReadAsync(scratch.AsMemory(0, Math.Min(remaining, scratch.Length)), ct);
                if (count == 0) throw new EndOfStreamException("truncated discovery message");
                remaining -= count;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(scratch); }
        return (command, []);
    }

    private async Task LoadAsync(CancellationToken ct)
    {
        if (_loaded) return;
        _loaded = true;
        try
        {
            if (!File.Exists(_cachePath)) return;
            if (new FileInfo(_cachePath).Length > 2 * 1024 * 1024)
                throw new InvalidDataException("discovery cache exceeds size limit");
            var cached = JsonSerializer.Deserialize<AutoPeerCandidate[]>(await File.ReadAllTextAsync(_cachePath, ct));
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var candidate in (cached ?? []).Take(MaxCandidates))
                if (P2pFastPeerManager.TryNormalizePublicEndpoint(candidate.Peer, null, out var peer) &&
                    candidate.AddressTimeMs >= now - 30 * 86_400_000L && candidate.AddressTimeMs <= now + 600_000)
                    _candidates[peer] = candidate with { Peer = peer, PingMs = null, SourcePriority = 1 };
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or JsonException)
        {
            SoloLog.Warn("p2p discovery cache ignored", ("error", ex.Message));
        }
    }

    private async Task SaveAsync(AutoPeerCandidate[] candidates, CancellationToken ct)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_cachePath)!);
        var temporary = _cachePath + ".tmp";
        try
        {
            await File.WriteAllTextAsync(temporary, JsonSerializer.Serialize(candidates), ct);
            File.Move(temporary, _cachePath, overwrite: true);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}
