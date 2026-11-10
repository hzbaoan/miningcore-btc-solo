using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.P2p;

/// <summary>
/// Minimal Bitcoin P2P client for early headers/cmpctblock announcements.
/// </summary>
public sealed class P2pFastPeer
{
    private readonly AppConfig _cfg;
    private readonly TemplateEngine _engine;
    private readonly BitcoinRpcClient _rpc;
    private const int ProtocolVersion = 70016;
    private const uint MsgBlock = 2;
    private const uint MsgCmpctBlock = 4;
    private const uint MsgWitnessFlag = 1u << 30;
    private const ulong ServiceNetwork = 1;
    private const ulong ServiceWitness = 1UL << 3;
    private const ulong ServiceNetworkLimited = 1UL << 10;
    private static readonly long FailureLogIntervalTicks = (long)(TimeSpan.FromMinutes(1).TotalSeconds * Stopwatch.Frequency);
    private static readonly TimeSpan TipCheckInterval = TimeSpan.FromSeconds(2);
    private readonly SemaphoreSlim _tipHeaderGate = new(1, 1);
    private string? _cachedTipHash;
    private byte[]? _cachedTipPayload;
    private long _tipHeaderRetryAfterMs;

    internal event Action<string, string, string?>? PeerStateChanged;
    internal event Action<string, double>? PeerTcpConnected;

    public P2pFastPeer(AppConfig cfg, TemplateEngine engine, BitcoinRpcClient rpc)
    {
        _cfg = cfg;
        _engine = engine;
        _rpc = rpc;
    }

    internal async Task RunPeerLoopAsync(string peer, CancellationToken ct)
    {
        var firstAttempt = true;
        var logState = new PeerLogState();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                PeerStateChanged?.Invoke(peer, "connecting", null);
                if (firstAttempt)
                {
                    SoloLog.Info("p2p fast peer connecting", ("peer", peer));
                    firstAttempt = false;
                }

                await RunSessionAsync(peer, ct, logState);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                PeerStateChanged?.Invoke(peer, "failed", ex.Message);
                var now = Stopwatch.GetTimestamp();
                if (logState.TryRecordFailure(now, out var failuresSinceLastLog))
                {
                    SoloLog.Warn("p2p fast peer session failed",
                        ("peer", peer),
                        ("error", ex.Message),
                        ("failures_since_last_log", failuresSinceLastLog));
                }
            }

            try
            {
                await Task.Delay(3000, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task RunSessionAsync(string peer, CancellationToken ct, PeerLogState logState)
    {
        var (host, port) = P2pFastPeerManager.ParseEndpoint(peer, _cfg.NetworkName, allowDefaultPort: true);
        using var client = new TcpClient();
        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        connectCts.CancelAfter(TimeSpan.FromSeconds(5));
        var connectTimer = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await client.ConnectAsync(host, port, connectCts.Token);
        }
        catch (OperationCanceledException ex) when (!ct.IsCancellationRequested && connectCts.IsCancellationRequested)
        {
            throw new TimeoutException($"P2P TCP connect to {peer} timed out after 5 seconds", ex);
        }
        connectTimer.Stop();
        PeerTcpConnected?.Invoke(peer, connectTimer.Elapsed.TotalMilliseconds);
        PeerStateChanged?.Invoke(peer, "handshaking", null);
        client.NoDelay = true;
        await using var stream = client.GetStream();
        using var writer = new PeerWriter(stream, NetworkMagic());
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var tipSignal = new SemaphoreSlim(0, 1);
        var sessionToken = sessionCts.Token;
        Task? tipTask = null;

        var remote = (IPEndPoint)client.Client.RemoteEndPoint!;
        SoloLog.Debug("p2p fast peer connected",
            ("peer", peer),
            ("remote", remote.ToString()));
        // Advertise a tip height so the peer treats us as caught-up (cmpct high-bandwidth).
        var startHeight = (int)Math.Max(0, (long)_engine.ActiveMiningJob.Height - 1);
        await writer.SendAsync("version", BuildVersionPayload(remote, startHeight), sessionToken);
        SoloLog.Debug("p2p fast version sent", ("peer", peer), ("start_height", startHeight));

        var sentVerack = false;
        var receivedVerack = false;
        var headerRequests = new HeaderRequestTracker();
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
        handshakeCts.CancelAfter(TimeSpan.FromSeconds(15));

        try
        {
            while (!sessionToken.IsCancellationRequested)
            {
                (string Command, byte[] Payload) msg;
                try
                {
                    msg = await ReadMessageAsync(
                        stream,
                        receivedVerack ? sessionToken : handshakeCts.Token,
                        header => receivedVerack
                            ? HandleHeaderBytesAsync(peer, header, sessionToken)
                            : Task.CompletedTask);
                }
                catch (OperationCanceledException ex) when (!sessionToken.IsCancellationRequested && handshakeCts.IsCancellationRequested)
                {
                    var waitingFor = sentVerack ? "verack" : "version";
                    throw new TimeoutException(
                        $"P2P handshake with {peer} timed out waiting for {waitingFor} after 15 seconds", ex);
                }
                switch (msg.Command)
                {
                    case "version":
                        SoloLog.Debug("p2p fast peer version received",
                            ("peer", peer), ("payload_bytes", msg.Payload.Length));
                        if (!sentVerack)
                        {
                            await writer.SendAsync("verack", Array.Empty<byte>(), sessionToken);
                            sentVerack = true;
                            SoloLog.Debug("p2p fast peer verack sent", ("peer", peer));
                        }
                        break;
                    case "verack":
                        if (receivedVerack)
                            break;
                        receivedVerack = true;
                        await writer.SendAsync("sendcmpct", BuildSendCmpct(2), sessionToken);
                        await writer.SendAsync("sendheaders", Array.Empty<byte>(), sessionToken);
                        // Tip RPC/cache work must never stop this socket from reading a new header.
                        tipTask = MaintainTipAnnouncementAsync(peer, writer, tipSignal, sessionCts);
                        PeerStateChanged?.Invoke(peer, "connected", null);
                        if (logState.TryLogReady())
                            SoloLog.Info("p2p fast peer ready", ("peer", peer), ("start_height", startHeight));
                        break;
                    case "ping":
                        if (msg.Payload.Length >= 8)
                            await writer.SendAsync("pong", msg.Payload.AsSpan(0, 8).ToArray(), sessionToken);
                        break;
                    case "getheaders" when receivedVerack:
                        if (IsValidGetHeadersPayload(msg.Payload))
                        {
                            try { tipSignal.Release(); }
                            catch (SemaphoreFullException) { }
                        }
                        break;
                    case "headers" when receivedVerack:
                        await HandleHeadersAsync(peer, msg.Payload, sessionToken);
                        break;
                    case "inv" when receivedVerack:
                        await HandleInvAsync(peer, writer, msg.Payload, headerRequests, sessionToken);
                        break;
                }
            }
        }
        finally
        {
            sessionCts.Cancel();
            if (tipTask != null)
            {
                try { await tipTask; }
                catch (OperationCanceledException) when (sessionCts.IsCancellationRequested) { }
            }
        }
    }

    private async Task HandleInvAsync(
        string peer,
        PeerWriter writer,
        byte[] payload,
        HeaderRequestTracker headerRequests,
        CancellationToken ct)
    {
        var pos = 0;
        if (!ReadVarInt(payload, ref pos, out var count) || count == 0 || count > 50_000)
            return;

        var job = _engine.ActiveMiningJob;
        if (!job.Ready || string.IsNullOrEmpty(job.PrevhashBe))
            return;

        var hasBlock = false;
        for (ulong i = 0; i < count; i++)
        {
            if (pos + 36 > payload.Length)
                return;
            var type = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(pos, 4));
            pos += 4;
            var isCurrentTip = payload.AsSpan(pos, 32).SequenceEqual(job.PrevhashLe);
            pos += 32; // hash (LE)

            // MSG_BLOCK / MSG_WITNESS_BLOCK / MSG_CMPCT_BLOCK
            var baseType = type & ~MsgWitnessFlag;
            if ((baseType is MsgBlock or MsgCmpctBlock) && !isCurrentTip)
                hasBlock = true;
        }

        if (pos != payload.Length)
            return;

        if (!hasBlock)
            return;

        // Solo: headers-first — request headers from our tip, not getdata/cmpctblock body.
        byte[] tipLe;
        try
        {
            tipLe = Hex.ReverseCopy(Hex.Decode(job.PrevhashBe));
        }
        catch
        {
            return;
        }

        // A peer may repeat the same block inventory while headers are already in
        // flight. Bound duplicate requests, but let a later inventory retry a locator
        // after an empty/lost response instead of suppressing it for the whole session.
        if (!headerRequests.TryRequest(job.PrevhashBe, Stopwatch.GetTimestamp()))
            return;

        using var outMs = new MemoryStream();
        using (var w = new BinaryWriter(outMs))
        {
            w.Write(ProtocolVersion);
            BitcoinEncoding.WriteVarInt(w, 1UL);
            w.Write(tipLe);
            w.Write(new byte[32]); // hash_stop = zero → all headers after locator
        }

        SoloLog.Info("p2p inv -> getheaders",
            ("peer", peer),
            ("locator", job.PrevhashBe));
        await writer.SendAsync("getheaders", outMs.ToArray(), ct);
    }

    private async Task HandleHeadersAsync(string peer, byte[] payload, CancellationToken ct)
    {
        if (!TryParseHeadersPayload(payload, out var announcements))
            throw new InvalidDataException("malformed p2p headers payload or count exceeds 2000");

        SoloLog.Info("p2p headers received",
            ("peer", peer),
            ("count", announcements.Length));

        var job = _engine.ActiveMiningJob;
        if (!job.Ready)
            return;

        for (var i = 0; i < announcements.Length; i++)
        {
            var annHeightValue = (ulong)job.Height + (uint)i;
            if (annHeightValue > uint.MaxValue)
                return;

            var ann = announcements[i];
            // Announced block height ≈ current job height (the block we were mining) + i
            var annHeight = (uint)annHeightValue;
            await HandleHeaderAnnouncementAsync(peer, ann, annHeight, ct);
        }
    }

    private static bool IsValidGetHeadersPayload(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4)
            return false;
        var pos = 4;
        if (!ReadVarInt(payload, ref pos, out var count) || count == 0 || count > 101)
            return false;
        var locatorBytes = checked((int)count * 32);
        return pos == payload.Length - locatorBytes - 32;
    }

    private async Task MaintainTipAnnouncementAsync(
        string peer,
        PeerWriter writer,
        SemaphoreSlim signal,
        CancellationTokenSource sessionCts)
    {
        var ct = sessionCts.Token;
        string? lastSentHash = null;
        var force = true;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Use a Core-confirmed tip: active work may already be a speculative
                // P2P child whose header our local Core does not yet have.
                var tip = _engine.AuthoritativeJob;
                if (tip.Ready && (force || !string.Equals(lastSentHash, tip.PrevhashBe, StringComparison.Ordinal)))
                {
                    var payload = await GetTipHeaderPayloadAsync(tip.PrevhashBe, ct);
                    if (payload != null)
                    {
                        if (_engine.AuthoritativeJob.PrevhashBe != tip.PrevhashBe)
                            continue;
                        await writer.SendAsync("headers", payload, ct);
                        lastSentHash = tip.PrevhashBe;
                        force = false;
                        SoloLog.Debug("p2p tip header announced", ("peer", peer), ("hash", lastSentHash));
                    }
                }
                force |= await signal.WaitAsync(TipCheckInterval, ct);
            }
        }
        catch
        {
            // A failed socket writer must also release the session's pending read.
            sessionCts.Cancel();
            throw;
        }
    }

    private async Task<byte[]?> GetTipHeaderPayloadAsync(string hash, CancellationToken ct)
    {
        // One RPC result per tip shared by every peer. Failures get a short cooldown
        // so an unavailable Core cannot cause one request per connected peer.
        await _tipHeaderGate.WaitAsync(ct);
        try
        {
            if (string.Equals(_cachedTipHash, hash, StringComparison.Ordinal) &&
                (_cachedTipPayload != null || Environment.TickCount64 < _tipHeaderRetryAfterMs))
                return _cachedTipPayload;

            _cachedTipHash = hash;
            _cachedTipPayload = null;
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(TimeSpan.FromSeconds(3));
                var headerHex = await _rpc.CallAsync<string>(
                    "getblockheader", new object[] { hash, false }, timeout.Token);
                var payload = new byte[82];
                payload[0] = 1; // one header; final byte is the zero transaction count
                if (headerHex.Length != 160 ||
                    !BitcoinEncoding.TryDecodeExactHex(headerHex.AsSpan(), payload.AsSpan(1, 80)))
                    throw new InvalidDataException("Core tip header must contain exactly 80 bytes");
                Span<byte> actualHash = stackalloc byte[32];
                Span<byte> expectedHash = stackalloc byte[32];
                BitcoinEncoding.DoubleSha256(payload.AsSpan(1, 80), actualHash);
                actualHash.Reverse();
                if (!BitcoinEncoding.TryDecodeExactHex(hash.AsSpan(), expectedHash) ||
                    !actualHash.SequenceEqual(expectedHash))
                    throw new InvalidDataException("Core tip header does not match the requested hash");
                _cachedTipPayload = payload;
                return payload;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex)
            {
                _tipHeaderRetryAfterMs = Environment.TickCount64 + 5000;
                SoloLog.Debug("p2p tip header fetch deferred", ("hash", hash), ("error", ex.Message));
                return null;
            }
        }
        finally { _tipHeaderGate.Release(); }
    }

    private async Task HandleHeaderBytesAsync(
        string peer,
        byte[] header80,
        CancellationToken ct)
    {
        if (header80.Length != 80)
            return;

        var job = _engine.ActiveMiningJob;
        if (!job.Ready)
            return;
        var ann = ParseHeader(header80);
        var annHeight = job.Height;
        await HandleHeaderAnnouncementAsync(peer, ann, annHeight, ct);
    }

    private Task HandleHeaderAnnouncementAsync(
        string peer,
        HeaderAnnouncement ann,
        uint annHeight,
        CancellationToken ct)
    {
        return _engine.HandleP2pFastAnnouncementAsync(
            ann.PrevhashHex,
            ann.BlockHashHex,
            ann.BlockTime,
            annHeight,
            ann.BlockNbits,
            ann.BlockVersion,
            ct,
            peer);
    }

    internal static bool TryParseHeadersPayload(
        ReadOnlySpan<byte> payload,
        out HeaderAnnouncement[] announcements)
    {
        announcements = Array.Empty<HeaderAnnouncement>();
        var pos = 0;
        if (!ReadVarInt(payload, ref pos, out var count) || count > 2000)
            return false;

        var parsed = new HeaderAnnouncement[(int)count];
        for (var i = 0; i < parsed.Length; i++)
        {
            if (pos > payload.Length - 80)
                return false;
            var announcement = ParseHeader(payload.Slice(pos, 80));
            pos += 80;
            if (!ReadVarInt(payload, ref pos, out var txnCount) || txnCount != 0)
                return false;
            if (i > 0 && !announcement.PrevhashHex.Equals(
                    parsed[i - 1].BlockHashHex, StringComparison.OrdinalIgnoreCase))
                return false;
            parsed[i] = announcement;
        }

        if (pos != payload.Length)
            return false;
        announcements = parsed;
        return true;
    }

    private static HeaderAnnouncement ParseHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length != 80)
            throw new ArgumentException("block header must be exactly 80 bytes", nameof(header));

        var version = BinaryPrimitives.ReadUInt32LittleEndian(header[..4]);
        Span<byte> prevBe = stackalloc byte[32];
        header.Slice(4, 32).CopyTo(prevBe);
        prevBe.Reverse();
        Span<byte> hashBe = stackalloc byte[32];
        BitcoinEncoding.DoubleSha256(header, hashBe);
        hashBe.Reverse();
        var time = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(68, 4));
        var nbits = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(72, 4));
        return new HeaderAnnouncement(
            Hex.Encode(prevBe), Hex.Encode(hashBe), version, time, nbits);
    }

    internal static byte[] BuildVersionPayload(IPEndPoint remote, int startHeight)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write(ProtocolVersion);
        w.Write(ServiceNetwork | ServiceWitness | ServiceNetworkLimited);
        w.Write(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        WriteNetAddr(w, remote);
        WriteNetAddr(w, new IPEndPoint(IPAddress.Loopback, 0));
        w.Write((long)(uint)RandomNumberGenerator.GetInt32(int.MaxValue) | ((long)(uint)RandomNumberGenerator.GetInt32(int.MaxValue) << 32));
        WriteVarString(w, AppInfo.P2pUserAgent);
        w.Write(startHeight);
        w.Write((byte)0); // This peer does not relay transactions.
        return ms.ToArray();
    }

    private static void WriteNetAddr(BinaryWriter w, IPEndPoint ep)
    {
        w.Write(ServiceNetwork | ServiceWitness);
        var ip = ep.Address.MapToIPv6().GetAddressBytes();
        w.Write(ip);
        // port big-endian
        w.Write((byte)(ep.Port >> 8));
        w.Write((byte)(ep.Port & 0xff));
    }

    private static void WriteVarString(BinaryWriter w, string s)
    {
        var bytes = Encoding.ASCII.GetBytes(s);
        BitcoinEncoding.WriteVarInt(w, (ulong)bytes.Length);
        w.Write(bytes);
    }

    internal static byte[] BuildSendCmpct(ulong version)
    {
        var buf = new byte[9];
        buf[0] = 1; // announce
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(1), version);
        return buf;
    }

    private sealed class PeerWriter(NetworkStream stream, uint magic) : IDisposable
    {
        private readonly SemaphoreSlim _gate = new(1, 1);

        public async Task SendAsync(string command, byte[] payload, CancellationToken ct)
        {
            // Tip announcements and receive-loop replies share this writer. A whole
            // frame is written under one gate, so their bytes can never interleave.
            var frame = new byte[checked(24 + payload.Length)];
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), magic);
            Encoding.ASCII.GetBytes(command.AsSpan(), frame.AsSpan(4, 12));
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(16, 4), (uint)payload.Length);
            BitcoinEncoding.DoubleSha256(payload).AsSpan(0, 4).CopyTo(frame.AsSpan(20, 4));
            payload.CopyTo(frame, 24);
            await _gate.WaitAsync(ct);
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                try { await stream.WriteAsync(frame, timeout.Token); }
                catch (OperationCanceledException ex) when (!ct.IsCancellationRequested)
                {
                    throw new TimeoutException("P2P socket write timed out after 5 seconds", ex);
                }
            }
            finally { _gate.Release(); }
        }

        public void Dispose() => _gate.Dispose();
    }

    internal async Task<(string Command, byte[] Payload)> ReadMessageAsync(
        NetworkStream stream,
        CancellationToken ct,
        Func<byte[], Task>? onEarlyBlockHeader = null)
    {
        var header = await ReadExactAsync(stream, 24, ct, "message header");
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        if (magic != NetworkMagic())
            throw new InvalidOperationException("bad p2p magic");
        var cmd = Encoding.ASCII.GetString(header, 4, 12).TrimEnd('\0');
        var len = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        if (len > 4 * 1024 * 1024)
            throw new InvalidOperationException("p2p payload too large");
        byte[] payload;
        Task? earlyHeaderTask = null;
        if (len == 0)
        {
            payload = Array.Empty<byte>();
        }
        else if (cmd is "block" or "cmpctblock")
        {
            // Fast-tip handling needs only the serialized 80-byte block header.
            // Publish as soon as the header arrives while the remainder is drained.
            (payload, earlyHeaderTask) =
                await ReadPrefixAndDiscardAsync(
                    stream,
                    (int)len,
                    80,
                    onEarlyBlockHeader,
                    ct,
                    cmd);
        }
        else
        {
            payload = await ReadExactAsync(stream, (int)len, ct, "payload", cmd);
        }

        if (earlyHeaderTask != null)
            await earlyHeaderTask;
        return (cmd, payload);
    }

    private static async Task<(byte[] Prefix, Task? PrefixTask)> ReadPrefixAndDiscardAsync(
        NetworkStream stream,
        int totalLength,
        int prefixLength,
        Func<byte[], Task>? onPrefix,
        CancellationToken ct,
        string command)
    {
        var keepLength = Math.Min(totalLength, prefixLength);
        var prefix = keepLength == 0
            ? Array.Empty<byte>()
            : await ReadExactAsync(stream, keepLength, ct, "block header", command);
        var prefixTask = prefix.Length == prefixLength ? onPrefix?.Invoke(prefix) : null;
        try
        {
            var remaining = totalLength - keepLength;
            if (remaining == 0)
                return (prefix, prefixTask);

            var scratch = ArrayPool<byte>.Shared.Rent(Math.Min(64 * 1024, remaining));
            try
            {
                while (remaining > 0)
                {
                    var n = await stream.ReadAsync(scratch.AsMemory(0, Math.Min(scratch.Length, remaining)), ct);
                    if (n == 0)
                        throw new EndOfStreamException(
                            $"P2P stream closed during {command} payload drain: received {totalLength - remaining} of {totalLength} bytes");
                    remaining -= n;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(scratch);
            }

            return (prefix, prefixTask);
        }
        catch
        {
            if (prefixTask != null)
            {
                try { await prefixTask; }
                catch { }
            }
            throw;
        }
    }

    private static async Task<byte[]> ReadExactAsync(
        NetworkStream stream, int len, CancellationToken ct, string stage, string? command = null)
    {
        var buf = new byte[len];
        var off = 0;
        while (off < len)
        {
            var n = await stream.ReadAsync(buf.AsMemory(off, len - off), ct);
            if (n == 0)
                throw new EndOfStreamException(
                    $"P2P stream closed during {command ?? "incoming"} {stage}: received {off} of {len} bytes");
            off += n;
        }
        return buf;
    }

    private uint NetworkMagic() => _cfg.NetworkName.ToLowerInvariant() switch
    {
        "mainnet" or "bitcoin" => 0xd9b4bef9,
        "testnet" => 0x0709110b,
        "regtest" => 0xdab5bffa,
        _ => throw new InvalidOperationException($"unsupported P2P network: {_cfg.NetworkName}")
    };

    private static bool ReadVarInt(ReadOnlySpan<byte> data, ref int pos, out ulong value)
    {
        value = 0;
        if (pos >= data.Length) return false;
        var prefix = data[pos++];
        if (prefix < 0xfd)
        {
            value = prefix;
            return true;
        }
        if (prefix == 0xfd)
        {
            if (pos + 2 > data.Length) return false;
            value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
            pos += 2;
            return value >= 0xfd;
        }
        if (prefix == 0xfe)
        {
            if (pos + 4 > data.Length) return false;
            value = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
            pos += 4;
            return value > ushort.MaxValue;
        }
        if (pos + 8 > data.Length) return false;
        value = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
        pos += 8;
        return value > uint.MaxValue;
    }

    internal readonly record struct HeaderAnnouncement(
        string PrevhashHex,
        string BlockHashHex,
        uint BlockVersion,
        uint BlockTime,
        uint BlockNbits);

    internal sealed class HeaderRequestTracker
    {
        private string? _lastLocator;
        private long _requestedAt;

        internal bool TryRequest(string locator, long timestamp)
        {
            if (string.Equals(locator, _lastLocator, StringComparison.OrdinalIgnoreCase) &&
                Stopwatch.GetElapsedTime(_requestedAt, timestamp) < TimeSpan.FromSeconds(5))
                return false;
            _lastLocator = locator;
            _requestedAt = timestamp;
            return true;
        }
    }

    internal sealed class PeerLogState
    {
        private bool _readyLogged;
        private long _nextFailureLogTimestamp;
        private int _failuresSinceLastLog;

        internal bool TryLogReady()
        {
            if (_readyLogged)
                return false;
            _readyLogged = true;
            return true;
        }

        internal bool TryRecordFailure(long timestamp, out int failuresSinceLastLog)
        {
            _failuresSinceLastLog++;
            if (_nextFailureLogTimestamp != 0 && timestamp < _nextFailureLogTimestamp)
            {
                failuresSinceLastLog = 0;
                return false;
            }

            failuresSinceLastLog = _failuresSinceLastLog;
            _failuresSinceLastLog = 0;
            _nextFailureLogTimestamp = timestamp + FailureLogIntervalTicks;
            return true;
        }
    }
}
