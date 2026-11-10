using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Stratum;

public sealed class StratumServer
{
    private const double NetworkDifficultySafetyFactor = 0.999999;

    private readonly AppConfig _cfg;
    private readonly TemplateEngine _engine;
    private readonly MetricsStore _metrics;
    private readonly ConcurrentDictionary<Guid, ClientSession> _clients = new();
    private readonly ConcurrentDictionary<Guid, Task> _clientTasks = new();
    private readonly object _cleanBarrierLock = new();
    private readonly List<CleanBroadcastBarrier> _cleanBarriers = new();
    private readonly ExtranonceLeasePool _extranonceLeases;
    private long _lastPrunedMinimumEpoch = long.MinValue;
    /// <summary>Subscribed miner count (avoids O(n) recount on connect/disconnect).</summary>
    private int _subscribedCount;

    public StratumServer(AppConfig cfg, TemplateEngine engine, MetricsStore metrics)
    {
        _cfg = cfg;
        _engine = engine;
        _metrics = metrics;
        _extranonceLeases = new ExtranonceLeasePool(cfg.Stratum.Extranonce1Size);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var ip = IPAddress.Parse(_cfg.Stratum.ListenAddr);
        using var listener = new TcpListener(ip, _cfg.Stratum.ListenPort);
        using var serviceCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var serviceCt = serviceCts.Token;
        listener.Start();
        SoloLog.Info("Stratum V1 listening",
            ("addr", $"{_cfg.Stratum.ListenAddr}:{_cfg.Stratum.ListenPort}"),
            ("idle_timeout_secs", Math.Max(0, _cfg.Stratum.IdleTimeoutSecs)),
            ("max_connections", _cfg.Stratum.MaxConnections),
            ("max_message_bytes", _cfg.Stratum.MaxMessageBytes),
            ("send_queue_capacity", _cfg.Stratum.SendQueueCapacity),
            ("write_timeout_secs", _cfg.Stratum.WriteTimeoutSecs),
            ("clean_broadcast_timeout_ms", _cfg.Stratum.CleanBroadcastTimeoutMs),
            ("late_share_grace_ms", _cfg.Stratum.LateShareGraceMs));

        var serviceTasks = new List<(string Name, Task Task)>
        {
            ("accept", AcceptLoop(listener, serviceCt)),
            ("broadcast", BroadcastLoop(serviceCt)),
            ("vardiff watchdog", VarDiffWatchdogLoop(serviceCt)),
            ("job retirement", JobRetirementLoop(serviceCt))
        };
        if (_cfg.Stratum.IdleTimeoutSecs > 0)
            serviceTasks.Add(("idle watchdog", IdleWatchdogLoop(serviceCt)));

        try
        {
            var completedTask = await Task.WhenAny(serviceTasks.Select(entry => entry.Task));
            if (ct.IsCancellationRequested)
                return;

            var completedService = serviceTasks.First(entry => ReferenceEquals(entry.Task, completedTask));
            await completedTask;
            throw new InvalidOperationException(
                $"Stratum {completedService.Name} loop stopped unexpectedly");
        }
        finally
        {
            serviceCts.Cancel();
            // Stop accepting first, then unblock every client read and await all handlers
            // before the submit queue is asked to drain.
            listener.Stop();
            foreach (var client in _clients.Values)
            {
                try { client.Tcp.Close(); } catch { }
            }

            var clientTasks = _clientTasks.Values.ToArray();
            try
            {
                await Task.WhenAll(clientTasks);
            }
            catch (Exception ex)
            {
                SoloLog.Warn("stratum client shutdown error", ("error", ex.Message));
            }

            try
            {
                await Task.WhenAll(serviceTasks.Select(entry => entry.Task));
            }
            catch (OperationCanceledException) when (serviceCt.IsCancellationRequested)
            {
                // normal shutdown, or cancellation after another loop failed
            }
            catch (Exception ex)
            {
                // The first failing task is rethrown above. Cleanup must not replace it.
                SoloLog.Debug("stratum background task stopped during cleanup", ("error", ex.Message));
            }
        }
    }

    private async Task AcceptLoop(TcpListener listener, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TcpClient tcp;
            try
            {
                tcp = await listener.AcceptTcpClientAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }

            if (_clients.Count >= _cfg.Stratum.MaxConnections)
            {
                var rejectPeer = FormatPeer(tcp);
                SoloLog.Warn("miner rejected: max connections",
                    ("peer", rejectPeer),
                    ("max", _cfg.Stratum.MaxConnections),
                    ("connections", _clients.Count));
                tcp.Close();
                continue;
            }
            tcp.NoDelay = true;
            try
            {
                tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }
            catch (SocketException)
            {
                // Application-level reads/writes still detect disconnects.
            }
            var session = new ClientSession(tcp, _cfg, _metrics);
            _clients[session.Id] = session;
            _metrics.SetConnections(_clients.Count);
            // peer=ip:port for DDoS / abuse triage (logs only; dashboard still en1+UA).
            SoloLog.Info("miner connected",
                ("peer", session.RemoteEndpoint),
                ("connections", _clients.Count));

            // Do not pass ct to Task.Run itself: a cancellation race must still run
            // HandleClientAsync's finally block and be visible to shutdown draining.
            var clientTask = Task.Run(() => HandleClientAsync(session, ct), CancellationToken.None);
            _clientTasks[session.Id] = clientTask;
            _ = clientTask.ContinueWith(
                completedTask =>
                {
                    _clientTasks.TryRemove(session.Id, out var ignored);
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Enforce stratum.idle_timeout_secs while a connection is still completing its
    /// handshake. Authorized miners are exempt because Stratum V1 does not require a
    /// client heartbeat and a valid miner may legitimately submit no shares for a while.
    /// </summary>
    private async Task IdleWatchdogLoop(CancellationToken ct)
    {
        var idleSecs = _cfg.Stratum.IdleTimeoutSecs;
        if (idleSecs <= 0)
            return;

        // Check a few times per idle window; floor 15s so we do not spin.
        var period = TimeSpan.FromSeconds(Math.Clamp(idleSecs / 4.0, 15, 120));
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(period, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }

            var cutoff = DateTimeOffset.UtcNow.AddSeconds(-idleSecs);
            foreach (var client in _clients.Values)
            {
                if (client.Subscribed && client.Authorized)
                    continue;
                if (client.LastActivityUtc > cutoff)
                    continue;
                try
                {
                    SoloLog.Info("miner idle timeout",
                        ("peer", client.RemoteEndpoint),
                        ("en1", client.Extranonce1Hex),
                        ("ua", client.UserAgent),
                        ("idle_secs", idleSecs));
                    // Unblocks ReadLineAsync in HandleClientAsync; finally cleans metrics.
                    client.Tcp.Close();
                }
                catch
                {
                    // already closing
                }
            }
        }
    }

    /// <summary>
    /// Drive steady VarDiff independently of share arrivals. In particular, a full
    /// window with zero accepted shares must lower difficulty instead of getting stuck.
    /// </summary>
    private async Task VarDiffWatchdogLoop(CancellationToken ct)
    {
        var window = Math.Max(1.0, _cfg.Difficulty.RetargetTimeSecs);
        var period = TimeSpan.FromSeconds(Math.Clamp(window / 4.0, 1.0, 15.0));
        using var timer = new PeriodicTimer(period);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                foreach (var client in _clients.Values)
                {
                    if (!client.Subscribed || !client.Authorized)
                        continue;

                    try
                    {
                        await RetargetAsync(client, allowBurst: false, ct);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        SoloLog.Warn("vardiff watchdog error",
                            ("peer", client.RemoteEndpoint),
                            ("en1", client.Extranonce1Hex),
                            ("error", ex.Message));
                        try { client.Tcp.Close(); } catch { }
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
    }

    private async Task BroadcastLoop(CancellationToken ct)
    {
        await _engine.DispatchNotificationsAsync(notify =>
        {
            // Serialize once. Queueing is non-blocking and every client has an
            // independent writer, so actual socket writes proceed in parallel.
            var notifyBytes = BuildMiningNotifyBytes(notify.Job, notify.CleanJobs);
            var cleanNotifyBytes = notify.CleanJobs
                ? notifyBytes
                : BuildMiningNotifyBytes(notify.Job, clean: true);
            var recipients = notify.CleanJobs ? new List<ClientSession>() : null;
            foreach (var client in _clients.Values)
            {
                try
                {
                    client.VarDiffLock.Wait();
                    try
                    {
                        // Subscription publishes this state while holding the same lock,
                        // after its response is queued. Authorization only gates submits.
                        if (!client.Subscribed)
                            continue;

                        var difficultyBytes = ApplyPendingDifficultyForJob(client, notify.Job);
                        client.RegisterWorkAssignment(
                            notify.Job.JobKey,
                            notify.Job,
                            client.Difficulty,
                            client.Target);
                        var versionBytes = client.VersionRolling
                            ? BuildVersionMaskBytes(client, notify.Job)
                            : null;
                        if (!client.TryQueueJob(
                                notify.Epoch, notify.CleanJobs, versionBytes,
                                notifyBytes, cleanNotifyBytes, difficultyBytes))
                            throw new IOException("stratum client writer is closed");
                        client.MarkLastSentWork(notify.Job);
                    }
                    finally
                    {
                        client.VarDiffLock.Release();
                    }
                    recipients?.Add(client);
                }
                catch
                {
                    // A failed server push is enough evidence to tear down an otherwise
                    // read-idle authorized session and release its connection slot.
                    try { client.Tcp.Close(); } catch { }
                }
            }

            if (notify.CleanJobs)
                TrackCleanBroadcast(notify.Epoch, recipients!);
        }, ct);
    }

    private void TrackCleanBroadcast(long epoch, IReadOnlyList<ClientSession> recipients)
    {
        var now = DateTimeOffset.UtcNow;
        _metrics.RecordCleanBroadcast();
        if (recipients.Count == 0)
        {
            _engine.MarkCleanBroadcastComplete(epoch, now);
            return;
        }

        var barrier = new CleanBroadcastBarrier(
            epoch,
            recipients.ToArray(),
            now.AddMilliseconds(_cfg.Stratum.CleanBroadcastTimeoutMs));
        lock (_cleanBarrierLock)
            _cleanBarriers.Add(barrier);
    }

    private async Task JobRetirementLoop(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(25));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var now = DateTimeOffset.UtcNow;
                CleanBroadcastBarrier[] barriers;
                lock (_cleanBarrierLock)
                    barriers = _cleanBarriers.ToArray();

                foreach (var barrier in barriers)
                {
                    var pending = barrier.Clients.Where(client =>
                        _clients.TryGetValue(client.Id, out var live) &&
                        ReferenceEquals(live, client) &&
                        !client.WriterUnavailable &&
                        client.LastWrittenJobEpoch < barrier.Epoch).ToArray();

                    if (pending.Length > 0 && now < barrier.Deadline)
                        continue;

                    if (pending.Length > 0)
                    {
                        _metrics.RecordCleanBroadcastClientTimeouts(pending.Length);
                        foreach (var client in pending)
                        {
                            SoloLog.Warn("clean job write timeout; disconnecting miner",
                                ("peer", client.RemoteEndpoint),
                                ("en1", client.Extranonce1Hex),
                                ("epoch", barrier.Epoch),
                                ("last_written_epoch", client.LastWrittenJobEpoch));
                            try { client.Tcp.Close(); } catch { }
                        }
                    }

                    lock (_cleanBarrierLock)
                    {
                        if (!_cleanBarriers.Remove(barrier))
                            continue;
                    }
                    _engine.MarkCleanBroadcastComplete(barrier.Epoch, now);
                }

                _engine.ReclaimRetiredJobs(now);
                var minimumEpoch = _engine.MinimumRetainedEpoch;
                if (minimumEpoch != Volatile.Read(ref _lastPrunedMinimumEpoch))
                {
                    foreach (var client in _clients.Values)
                    {
                        client.PruneSubmittedSharesBefore(minimumEpoch);
                        client.PruneWorkAssignmentsBefore(minimumEpoch);
                    }
                    Volatile.Write(ref _lastPrunedMinimumEpoch, minimumEpoch);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
    }

    private async Task HandleClientAsync(ClientSession session, CancellationToken ct)
    {
        var reader = PipeReader.Create(session.Stream, new StreamPipeReaderOptions(
            bufferSize: 8192,
            minimumReadSize: 512,
            leaveOpen: true));
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                while (TryReadLine(ref buffer, out var line))
                {
                    if (line.Length > _cfg.Stratum.MaxMessageBytes)
                        throw new InvalidDataException(
                            $"stratum message exceeds {_cfg.Stratum.MaxMessageBytes} bytes");
                    if (IsWhiteSpace(line))
                        continue;

                    session.LastActivityUtc = DateTimeOffset.UtcNow;
                    await ProcessLineAsync(session, line, ct);
                }

                if (buffer.Length > _cfg.Stratum.MaxMessageBytes)
                    throw new InvalidDataException(
                        $"stratum message exceeds {_cfg.Stratum.MaxMessageBytes} bytes before newline");

                if (result.IsCompleted)
                {
                    if (!buffer.IsEmpty && !IsWhiteSpace(buffer))
                    {
                        session.LastActivityUtc = DateTimeOffset.UtcNow;
                        await ProcessLineAsync(session, buffer, ct);
                    }
                    reader.AdvanceTo(buffer.End);
                    break;
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        catch (Exception ex)
        {
            // Idle close and normal TCP reset often surface as ObjectDisposed / IO — keep noise down.
            if (!IsBenignDisconnect(ex))
            {
                SoloLog.Warn("stratum client error",
                    ("peer", session.RemoteEndpoint),
                    ("en1", session.Extranonce1Hex),
                    ("ua", session.UserAgent),
                    ("error", ex.Message));
            }
        }
        finally
        {
            await reader.CompleteAsync();
            _clients.TryRemove(session.Id, out _);
            if (session.Subscribed)
            {
                var n = Interlocked.Decrement(ref _subscribedCount);
                if (n < 0)
                    Interlocked.Exchange(ref _subscribedCount, 0);
            }
            _metrics.RemoveSession(session.SessionId);
            SoloLog.Info("miner disconnected",
                ("peer", session.RemoteEndpoint),
                ("en1", session.Extranonce1Hex),
                ("ua", session.UserAgent),
                ("authorized", session.Authorized),
                ("shares", session.AcceptedShares),
                ("best_diff", session.BestDiff.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
                ("connections", _clients.Count));
            await session.StopWriterAsync();
            if (session.HasExtranonceLease)
            {
                _extranonceLeases.Release(session.Extranonce1);
                session.HasExtranonceLease = false;
            }
            session.Dispose();
            _metrics.SetConnections(_clients.Count);
            _metrics.SetSubscriptions(Volatile.Read(ref _subscribedCount));
        }
    }

    private static bool IsBenignDisconnect(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException!)
        {
            if (e is ObjectDisposedException or IOException or SocketException or OperationCanceledException)
                return true;
        }
        return false;
    }

    private async Task ProcessLineAsync(
        ClientSession session,
        ReadOnlySequence<byte> line,
        CancellationToken ct)
    {
        if (TryParseSubmit(line, out var submit))
        {
            await HandleParsedSubmitAsync(session, submit, ct);
            return;
        }

        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        var method = root.TryGetProperty("method", out var m) ? m.GetString() : null;
        // The document remains alive until the awaited handler completes, so Clone() only
        // adds a per-request allocation without extending the element lifetime.
        var id = root.TryGetProperty("id", out var idEl) ? idEl : default;

        switch (method)
        {
            case "mining.subscribe":
                await HandleSubscribe(session, id, root);
                break;
            case "mining.authorize":
                await HandleAuthorize(session, id, root);
                break;
            case "mining.submit":
                LogRejectedShare(session, default, "malformed_submit_request");
                await WriteStratumErrorAsync(session, id, 20, "Invalid params");
                break;
            case "mining.configure":
                await HandleConfigure(session, id, root);
                break;
            case "mining.suggest_difficulty":
                await HandleSuggestDifficulty(session, id, root);
                break;
            case "mining.extranonce.subscribe":
                await WriteOkTrueAsync(session, id);
                break;
            case "mining.get_version":
                await WriteAsync(session, Ok(id, AppInfo.ProtocolVersion));
                break;
            case "mining.ping":
                await WriteAsync(session, Ok(id, "pong"));
                break;
            default:
                if (method != null)
                    await WriteStratumErrorAsync(session, id, 20, $"Unsupported method {method}");
                break;
        }
    }

    internal static bool TryParseSubmit(ReadOnlySequence<byte> line, out ParsedSubmit submit)
    {
        var parsed = new ParsedSubmitBuilder
        {
            Id = StratumRequestId.Null,
            ParamsTypesValid = true
        };
        var isSubmit = false;
        var rootEnded = false;
        var reader = new Utf8JsonReader(line, isFinalBlock: true, state: default);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            submit = default;
            return false;
        }

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                rootEnded = true;
                break;
            }
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("expected a JSON object property");

            var isId = reader.ValueTextEquals("id"u8);
            var isMethod = reader.ValueTextEquals("method"u8);
            var isParams = reader.ValueTextEquals("params"u8);
            if (!reader.Read())
                throw new JsonException("missing JSON property value");

            if (isId)
            {
                parsed.Id = ParseRequestId(ref reader);
            }
            else if (isMethod)
            {
                isSubmit = reader.TokenType == JsonTokenType.String &&
                    reader.ValueTextEquals("mining.submit"u8);
            }
            else if (isParams)
            {
                ParseSubmitParams(ref reader, ref parsed);
            }
            else if (reader.TokenType is JsonTokenType.StartArray or JsonTokenType.StartObject)
            {
                reader.Skip();
            }
        }

        if (!rootEnded)
            throw new JsonException("unterminated JSON object");
        if (reader.Read())
            throw new JsonException("trailing content after JSON object");

        parsed.HasRequiredParams = parsed.ParamsCount is 5 or 6 && parsed.ParamsTypesValid;
        submit = parsed.Build();
        return isSubmit;
    }

    private static void ParseSubmitParams(ref Utf8JsonReader reader, ref ParsedSubmitBuilder parsed)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
        {
            parsed.ParamsTypesValid = false;
            if (reader.TokenType == JsonTokenType.StartObject)
                reader.Skip();
            return;
        }

        var index = 0;
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                parsed.ParamsCount = index;
                return;
            }

            switch (index)
            {
                case 1:
                    if (reader.TokenType != JsonTokenType.String)
                        parsed.ParamsTypesValid = false;
                    else
                        parsed.ParamsTypesValid &= TryParseHexString(
                            ref reader, 16, stripMarkersAnywhere: false,
                            out parsed.JobKey, out _);
                    break;
                case 2:
                    if (reader.TokenType != JsonTokenType.String)
                    {
                        parsed.ParamsTypesValid = false;
                    }
                    else
                    {
                        parsed.Extranonce2Valid = TryParseHexString(
                            ref reader, 16, stripMarkersAnywhere: true,
                            out parsed.Extranonce2, out parsed.Extranonce2HexLength);
                    }
                    break;
                case 3:
                    if (reader.TokenType != JsonTokenType.String)
                    {
                        parsed.ParamsTypesValid = false;
                    }
                    else
                    {
                        parsed.NtimeValid = TryParseHexString(
                            ref reader, 8, stripMarkersAnywhere: false,
                            out var ntime, out _);
                        parsed.Ntime = (uint)ntime;
                    }
                    break;
                case 4:
                    if (reader.TokenType != JsonTokenType.String)
                    {
                        parsed.ParamsTypesValid = false;
                    }
                    else
                    {
                        parsed.NonceValid = TryParseHexString(
                            ref reader, 8, stripMarkersAnywhere: false,
                            out var nonce, out _);
                        parsed.Nonce = (uint)nonce;
                    }
                    break;
                case 5:
                    parsed.HasVersion = true;
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        parsed.VersionValid = TryParseExactU32Hex(ref reader, out var version);
                        parsed.Version = version;
                    }
                    break;
            }

            if (reader.TokenType is JsonTokenType.StartArray or JsonTokenType.StartObject)
                reader.Skip();
            index++;
        }

        throw new JsonException("unterminated submit params");
    }

    private static StratumRequestId ParseRequestId(ref Utf8JsonReader reader)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => StratumRequestId.Null,
            JsonTokenType.Number when reader.TryGetInt64(out var signed) =>
                StratumRequestId.FromInt64(signed),
            JsonTokenType.Number when reader.TryGetUInt64(out var unsigned) =>
                StratumRequestId.FromUInt64(unsigned),
            JsonTokenType.Number => ParseFloatingRequestId(ref reader),
            JsonTokenType.String => StratumRequestId.FromString(reader.GetString() ?? ""),
            _ => ParseRawRequestId(ref reader)
        };
    }

    private static StratumRequestId ParseFloatingRequestId(ref Utf8JsonReader reader) =>
        reader.TryGetDouble(out var value) && double.IsFinite(value)
            ? StratumRequestId.FromDouble(value)
            : ParseRawRequestId(ref reader);

    private static StratumRequestId ParseRawRequestId(ref Utf8JsonReader reader)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        return StratumRequestId.FromRawJson(document.RootElement.GetRawText());
    }

    private static bool TryParseHexString(
        ref Utf8JsonReader reader,
        int maxDigits,
        bool stripMarkersAnywhere,
        out ulong value,
        out int digitCount)
    {
        if (reader.ValueIsEscaped)
            return TryParseHexText(
                (reader.GetString() ?? "").AsSpan(), maxDigits, stripMarkersAnywhere,
                out value, out digitCount);

        if (!reader.HasValueSequence)
            return TryParseHexUtf8(
                reader.ValueSpan, maxDigits, stripMarkersAnywhere, out value, out digitCount);

        if (reader.ValueSequence.Length > 128)
        {
            value = 0;
            digitCount = reader.ValueSequence.Length > int.MaxValue
                ? int.MaxValue
                : (int)reader.ValueSequence.Length;
            return false;
        }

        Span<byte> bytes = stackalloc byte[(int)reader.ValueSequence.Length];
        reader.ValueSequence.CopyTo(bytes);
        return TryParseHexUtf8(bytes, maxDigits, stripMarkersAnywhere, out value, out digitCount);
    }

    private static bool TryParseExactU32Hex(ref Utf8JsonReader reader, out uint value)
    {
        value = 0;
        if (reader.TokenType != JsonTokenType.String)
            return false;
        if (reader.ValueIsEscaped)
            return TryParseVersionRollingMask(reader.GetString(), out value);

        if (!reader.HasValueSequence)
            return TryParseExactU32Hex(reader.ValueSpan, out value);
        if (reader.ValueSequence.Length != 8)
            return false;

        Span<byte> bytes = stackalloc byte[8];
        reader.ValueSequence.CopyTo(bytes);
        return TryParseExactU32Hex(bytes, out value);
    }

    private static bool TryParseExactU32Hex(ReadOnlySpan<byte> text, out uint value)
    {
        value = 0;
        if (text.Length != 8)
            return false;
        foreach (var character in text)
        {
            if (!TryHexNibble(character, out var nibble))
                return false;
            value = (value << 4) | (uint)nibble;
        }
        return true;
    }

    private static bool TryParseHexUtf8(
        ReadOnlySpan<byte> text,
        int maxDigits,
        bool stripMarkersAnywhere,
        out ulong value,
        out int digitCount)
    {
        var start = 0;
        var end = text.Length;
        while (start < end && IsAsciiWhiteSpace(text[start])) start++;
        while (end > start && IsAsciiWhiteSpace(text[end - 1])) end--;

        value = 0;
        digitCount = 0;
        var valid = true;
        for (var i = start; i < end; i++)
        {
            if ((stripMarkersAnywhere || digitCount == 0) && i + 1 < end &&
                text[i] == (byte)'0' && (text[i + 1] == (byte)'x' || text[i + 1] == (byte)'X'))
            {
                i++;
                continue;
            }

            digitCount++;
            if (!TryHexNibble(text[i], out var nibble) || digitCount > maxDigits)
            {
                valid = false;
                continue;
            }
            value = (value << 4) | nibble;
        }

        return valid && (stripMarkersAnywhere || digitCount > 0);
    }

    private static bool TryParseHexText(
        ReadOnlySpan<char> text,
        int maxDigits,
        bool stripMarkersAnywhere,
        out ulong value,
        out int digitCount)
    {
        text = text.Trim();
        value = 0;
        digitCount = 0;
        var valid = true;
        for (var i = 0; i < text.Length; i++)
        {
            if ((stripMarkersAnywhere || digitCount == 0) && i + 1 < text.Length &&
                text[i] == '0' && (text[i + 1] == 'x' || text[i + 1] == 'X'))
            {
                i++;
                continue;
            }

            digitCount++;
            var c = text[i];
            var nibble = c switch
            {
                >= '0' and <= '9' => c - '0',
                >= 'a' and <= 'f' => c - 'a' + 10,
                >= 'A' and <= 'F' => c - 'A' + 10,
                _ => -1
            };
            if (nibble < 0 || digitCount > maxDigits)
            {
                valid = false;
                continue;
            }
            value = (value << 4) | (uint)nibble;
        }

        return valid && (stripMarkersAnywhere || digitCount > 0);
    }

    private static bool TryHexNibble(byte value, out ulong nibble)
    {
        if (value is >= (byte)'0' and <= (byte)'9')
            nibble = (ulong)(value - (byte)'0');
        else if (value is >= (byte)'a' and <= (byte)'f')
            nibble = (ulong)(value - (byte)'a' + 10);
        else if (value is >= (byte)'A' and <= (byte)'F')
            nibble = (ulong)(value - (byte)'A' + 10);
        else
        {
            nibble = 0;
            return false;
        }
        return true;
    }

    private static bool IsAsciiWhiteSpace(byte value) =>
        value is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';

    private async Task HandleSubscribe(ClientSession session, JsonElement id, JsonElement root)
    {
        var userAgent = session.UserAgent;
        if (root.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array && p.GetArrayLength() > 0)
            userAgent = p[0].GetString() ?? "Unknown";

        await session.VarDiffLock.WaitAsync();
        try
        {
            if (!session.HasExtranonceLease)
            {
                if (!_extranonceLeases.TryAcquire(out var extranonce1))
                    throw new IOException("stratum extranonce1 keyspace is exhausted");
                session.Extranonce1 = extranonce1;
                session.Extranonce1Bytes = EncodeExtranonce1(
                    extranonce1, _cfg.Stratum.Extranonce1Size);
                session.Extranonce1Hex = Hex.Encode(session.Extranonce1Bytes);
                session.HasExtranonceLease = true;
            }

            var result = new object[]
            {
                new object[]
                {
                    new object[] { "mining.notify", session.SessionId },
                    new object[] { "mining.set_difficulty", session.SessionId }
                },
                session.Extranonce1Hex,
                _cfg.Stratum.Extranonce2Size
            };

            session.UserAgent = userAgent;
            session.ResetSubscriptionCaches();
            session.DiscardPendingJob();

            // The response must occupy the ordered stream before this connection becomes
            // visible to broadcast. The selected job and its difficulty are then bound in
            // one engine snapshot while the broadcaster waits on VarDiffLock.
            await WriteAsync(session, Ok(id, result));
            if (!session.Subscribed)
            {
                session.Subscribed = true;
                Interlocked.Increment(ref _subscribedCount);
            }

            var queuedJob = _engine.TryUseActiveMiningJob(job =>
            {
                session.ResetDifficulty(
                    ClampDifficultyForJob(_cfg.Difficulty, _cfg.Difficulty.Default, job),
                    DateTimeOffset.UtcNow);
                QueueAssignedJob(
                    session,
                    job,
                    clean: true,
                    BuildSetDifficultyBytes(session.Difficulty));
            });
            if (!queuedJob)
            {
                session.ResetDifficulty(
                    ClampDifficultyForJob(
                        _cfg.Difficulty,
                        _cfg.Difficulty.Default,
                        JobTemplate.Empty()),
                    DateTimeOffset.UtcNow);
                QueueDifficulty(session, session.Difficulty);
            }
        }
        finally
        {
            session.VarDiffLock.Release();
        }

        var subs = Volatile.Read(ref _subscribedCount);
        _metrics.SetSubscriptions(subs);
        _metrics.TouchWorker(ToWorkerIdentity(session), session.Difficulty);

        // peer for DDoS triage; en1+ua for miner identity (username / BTC address never logged).
        SoloLog.Info("miner subscribe",
            ("peer", session.RemoteEndpoint),
            ("en1", session.Extranonce1Hex),
            ("ua", session.UserAgent),
            ("difficulty", session.Difficulty.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
            ("subscriptions", subs));
    }

    internal static byte[] EncodeExtranonce1(uint value, int size)
    {
        var bytes = new byte[size];
        for (var i = 0; i < bytes.Length; i++)
            bytes[bytes.Length - 1 - i] = (byte)(value >> (i * 8));
        return bytes;
    }

    private async Task HandleAuthorize(ClientSession session, JsonElement id, JsonElement root)
    {
        // True solo: no password auth required. Username is worker label only (never logged).
        if (root.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array && p.GetArrayLength() > 0)
            session.Worker = p[0].GetString() ?? "worker";
        session.InvalidateWorkerIdentity();
        session.Authorized = true;
        await WriteOkTrueAsync(session, id);
        if (session.Subscribed)
        {
            await session.VarDiffLock.WaitAsync();
            try
            {
                _engine.TryUseActiveMiningJob(job =>
                {
                    if (session.LastSentWorkMatches(job))
                        return;

                    QueueAssignedJob(
                        session,
                        job,
                        clean: true);
                });
            }
            finally
            {
                session.VarDiffLock.Release();
            }
        }
        _metrics.TouchWorker(ToWorkerIdentity(session), session.Difficulty);
        // No per-authorize INFO: high churn with many miners; subscribe already logs en1+ua.
        // Username (often a BTC address) is never written to the log.
    }

    private async Task HandleConfigure(ClientSession session, JsonElement id, JsonElement root)
    {
        await session.VarDiffLock.WaitAsync();
        try
        {
            const uint allowed = 0x1fffe000;
            var mask = allowed;
            var advertisedMask = session.VersionMask;
            var activateVersionRolling = false;
            var result = new Dictionary<string, object>();
            if (root.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array)
            {
                var extensions = p.GetArrayLength() > 0 && p[0].ValueKind == JsonValueKind.Array
                    ? p[0].EnumerateArray().Select(x => x.GetString()).Where(x => x != null).ToList()
                    : new List<string?>();
                var extensionParameters = p.GetArrayLength() > 1 ? p[1] : default;
                var versionParametersValid = TryReadVersionRollingParameters(
                    extensionParameters, allowed, out mask);
                foreach (var ext in extensions)
                {
                    if (ext == "version-rolling")
                    {
                        if (!versionParametersValid)
                        {
                            result[ext] = "invalid version-rolling parameters";
                            continue;
                        }

                        advertisedMask = EffectiveVersionMask(mask, _engine.ActiveMiningJob);
                        result[ext!] = true;
                        result["version-rolling.mask"] = advertisedMask.ToString("x8");
                        activateVersionRolling = true;
                    }
                    else if (ext != null)
                        result[ext] = false;
                }
            }
            if (!activateVersionRolling && session.VersionRolling)
                advertisedMask = EffectiveVersionMask(session.VersionMask, _engine.ActiveMiningJob);

            // The response is sequenced before activation. A concurrent broadcaster uses
            // this same lock, so no version-mask notification or job can overtake it.
            await WriteAsync(session, Ok(id, result));
            if (activateVersionRolling)
            {
                session.VersionMask = mask;
                session.VersionRolling = true;
            }
            if (session.VersionRolling)
            {
                await WriteAsync(session, Notify(
                    "mining.set_version_mask", new object[] { advertisedMask.ToString("x8") }));
            }
        }
        finally
        {
            session.VarDiffLock.Release();
        }
    }

    internal static bool TryReadVersionRollingParameters(
        JsonElement parameters,
        uint allowedMask,
        out uint negotiatedMask)
    {
        negotiatedMask = allowedMask;
        if (parameters.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        // BIP310 marks this advisory capability value as required, but ASICs
        // in the field commonly omit it. Use the conventional two-bit value
        // for that compatibility case; malformed or negative values remain
        // invalid. The value is diagnostic only and does not widen the mask.
        if (parameters.TryGetProperty("version-rolling.min-bit-count", out var minBits) &&
            (minBits.ValueKind != JsonValueKind.Number ||
             !minBits.TryGetInt32(out var minimumBitCount) || minimumBitCount < 0))
        {
            return false;
        }

        if (!parameters.TryGetProperty("version-rolling.mask", out var requestedMask))
            return true;
        if (requestedMask.ValueKind != JsonValueKind.String ||
            !TryParseVersionRollingMask(requestedMask.GetString(), out var parsedMask))
        {
            return false;
        }

        negotiatedMask = parsedMask & allowedMask;
        return true;
    }

    private async Task HandleSuggestDifficulty(ClientSession session, JsonElement id, JsonElement root)
    {
        if (root.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array && p.GetArrayLength() > 0)
        {
            var d = p[0].ValueKind == JsonValueKind.Number ? p[0].GetDouble() : 0;
            if (d > 0)
            {
                var next = Math.Clamp(d, _cfg.Difficulty.Min, _cfg.Difficulty.Max);
                await session.VarDiffLock.WaitAsync();
                try
                {
                    session.ResetVarDiffWindow(DateTimeOffset.UtcNow);
                    session.SetPendingDifficulty(next);
                }
                finally
                {
                    session.VarDiffLock.Release();
                }
            }
        }
        await WriteOkTrueAsync(session, id);
    }

    private async Task HandleParsedSubmitAsync(
        ClientSession session,
        ParsedSubmit submit,
        CancellationToken ct)
    {
        var submitStartedTimestamp = Stopwatch.GetTimestamp();
        if (!session.Subscribed || !session.Authorized)
        {
            LogRejectedShare(session, submit, "unauthorized");
            await WriteStratumErrorAsync(session, submit.Id, 24, "Unauthorized");
            return;
        }

        if (!submit.HasRequiredParams ||
            !HasValidVersionParameterShape(
                session.VersionRolling, submit.HasVersion, submit.VersionValid))
        {
            var reason = !submit.HasRequiredParams
                ? "invalid_parameter_count_or_type"
                : session.VersionRolling && !submit.HasVersion
                    ? "missing_version_bits"
                    : !session.VersionRolling && submit.HasVersion
                        ? "unexpected_version_bits"
                        : "invalid_version_bits";
            LogRejectedShare(session, submit, reason);
            await WriteStratumErrorAsync(session, submit.Id, 20, "Invalid params");
            return;
        }

        if (!session.TryGetWorkAssignment(submit.JobKey, out var assignment))
        {
            if (session.TryGetRetiredWorkTemplateKey(submit.JobKey, out var retiredTemplateKey) &&
                _engine.FindJob(retiredTemplateKey).Status == JobLookupStatus.Expired)
            {
                LogRejectedShare(session, submit, "stale_job");
                await WriteStratumErrorAsync(session, submit.Id, 21, "Stale job");
                _metrics.RecordStaleShare();
                _metrics.RecordShareError();
                return;
            }

            LogRejectedShare(session, submit, "unknown_job");
            await WriteStratumErrorAsync(session, submit.Id, 21, "Job not found");
            _metrics.RecordUnknownJobShare();
            _metrics.RecordShareError();
            return;
        }

        var lookup = _engine.FindJob(assignment.TemplateJobKey);
        if (lookup.Status is JobLookupStatus.Expired)
        {
            LogRejectedShare(session, submit, "stale_job", lookup.Job, assignment);
            await WriteStratumErrorAsync(session, submit.Id, 21, "Stale job");
            _metrics.RecordStaleShare();
            _metrics.RecordShareError();
            return;
        }
        if (lookup.Job == null)
        {
            LogRejectedShare(session, submit, "unknown_job", assignment: assignment);
            await WriteStratumErrorAsync(session, submit.Id, 21, "Job not found");
            _metrics.RecordUnknownJobShare();
            _metrics.RecordShareError();
            return;
        }
        var job = lookup.Job;

        if (submit.HasVersion &&
            !AreSubmittedVersionBitsValid(submit.Version, session.VersionMask, job))
        {
            LogRejectedShare(session, submit, "version_bits_outside_mask", job, assignment);
            await WriteStratumErrorAsync(session, submit.Id, 20, "Invalid params");
            return;
        }

        ShareResult? result;
        ShareValidationFailure validationFailure;
        var shareRegistration = AcceptedShareRegistration.Added;

        // Keep validation, target selection and VarDiff observation in one session-critical
        // section so the periodic watchdog cannot change difficulty between those steps.
        await session.VarDiffLock.WaitAsync(ct);
        try
        {
            // Parsing and hashing stay in a synchronous helper so stack spans never cross an await.
            var validationStartedTimestamp = Stopwatch.GetTimestamp();
            try
            {
                result = ValidateShare(
                    session, job, submit, assignment.Target.LittleEndian, out validationFailure);
            }
            finally
            {
                _metrics.RecordShareValidation(Stopwatch.GetTimestamp() - validationStartedTimestamp);
            }
            if (validationFailure == ShareValidationFailure.None &&
                result is { } validated && (validated.Accepted || validated.IsBlock))
            {
                shareRegistration = session.TryRegisterSubmittedShare(
                    job.Epoch, validated.Hash, validated.IsBlock);
                if (shareRegistration == AcceptedShareRegistration.Added)
                {
                    if (validated.IsBlock && validated.BlockCandidate is { } blockCandidate)
                    {
                        var blockHashHex = validated.Hash.ToHex();
                        SoloLog.Alert("BLOCK FOUND",
                            ("peer", session.RemoteEndpoint),
                            ("en1", session.Extranonce1Hex),
                            ("ua", session.UserAgent),
                            ("height", job.Height),
                            ("hash", blockHashHex),
                            ("diff", validated.ActualDiff.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)));
                        try
                        {
                            // Memory-only enqueue must succeed before ACK. A full/closed
                            // client send queue can no longer discard a found block.
                            await _engine.SubmitBlockAsync(blockCandidate, blockHashHex, job.Height);
                        }
                        catch (Exception ex)
                        {
                            session.UnregisterSubmittedShare(job.Epoch, validated.Hash);
                            SoloLog.Alert("failed to enqueue found block",
                                ("height", job.Height),
                                ("hash", blockHashHex),
                                ("error", ex.Message));
                            throw;
                        }
                    }

                    // Duplicate ownership and any found-block enqueue are complete.
                    // Queue the ACK before dashboard/hashrate telemetry so a global
                    // metrics snapshot cannot extend miner-visible response latency.
                    await WriteAcceptedShareOkTrueAsync(session, submit.Id, submitStartedTimestamp);

                    var creditDiff = assignment.Difficulty;
                    var assignedDifficulty = session.Difficulty;
                    session.RecordAcceptedShare(creditDiff, validated.ActualDiff);
                    if (lookup.Status == JobLookupStatus.RetiredWithinGrace)
                        _metrics.RecordLateShare();
                    _metrics.RecordShare(
                        ToWorkerIdentity(session), creditDiff, validated.ActualDiff, true,
                        assignedDifficulty, validated.Hash.ToHex());
                    ct.ThrowIfCancellationRequested();
                    await RetargetLockedAsync(session, allowBurst: true);
                }
            }
        }
        finally
        {
            session.VarDiffLock.Release();
        }

        if (validationFailure != ShareValidationFailure.None)
        {
            LogRejectedShare(
                session, submit, ValidationFailureName(validationFailure), job, assignment, result);
            if (validationFailure is ShareValidationFailure.Extranonce2TooLong or
                ShareValidationFailure.InvalidExtranonce2)
                await WriteStratumErrorAsync(session, submit.Id, 20, "Invalid extranonce2");
            else
                await WriteStratumErrorAsync(session, submit.Id, 23, "Low difficulty share");
            _metrics.RecordShareError();
            return;
        }

        if (result is not { } completed || (!completed.Accepted && !completed.IsBlock))
        {
            LogRejectedShare(session, submit, "low_difficulty", job, assignment, result);
            await WriteStratumErrorAsync(session, submit.Id, 23, "Low difficulty share");
            _metrics.RecordShareError();
            return;
        }

        if (shareRegistration == AcceptedShareRegistration.Duplicate)
        {
            LogRejectedShare(session, submit, "duplicate_share", job, assignment, completed);
            await WriteStratumErrorAsync(session, submit.Id, 22, "Duplicate share");
            _metrics.RecordShareError();
            return;
        }

        if (shareRegistration == AcceptedShareRegistration.CapacityExceeded)
        {
            LogRejectedShare(session, submit, "share_tracking_capacity_exceeded", job, assignment, completed);
            await WriteStratumErrorAsync(session, submit.Id, 20, "Share tracking capacity exceeded");
            _metrics.RecordShareError();
            return;
        }

    }

    private ShareResult? ValidateShare(
        ClientSession session,
        JobTemplate job,
        ParsedSubmit submit,
        ReadOnlySpan<byte> shareTargetLe,
        out ShareValidationFailure failure)
    {
        var expectedBytes = _cfg.Stratum.Extranonce2Size;
        failure = ShareValidationFailure.None;
        if (submit.Extranonce2HexLength > expectedBytes * 2)
        {
            failure = ShareValidationFailure.Extranonce2TooLong;
            return null;
        }

        Span<byte> en2 = stackalloc byte[8];
        if (!submit.TryWriteExtranonce2(expectedBytes, en2))
        {
            failure = ShareValidationFailure.InvalidExtranonce2;
            return null;
        }
        if (!submit.NtimeValid)
        {
            failure = ShareValidationFailure.InvalidNtime;
            return null;
        }
        if (!submit.NonceValid)
        {
            failure = ShareValidationFailure.InvalidNonce;
            return null;
        }
        if (job.Mintime != 0 && submit.Ntime < job.Mintime)
        {
            failure = ShareValidationFailure.NtimeBeforeMintime;
            return null;
        }
        if ((ulong)submit.Ntime > (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 7200)
        {
            failure = ShareValidationFailure.NtimeTooFarInFuture;
            return null;
        }

        var version = job.Version;
        if (session.VersionRolling && submit.HasVersion && submit.VersionValid)
        {
            var rollMask = EffectiveVersionMask(session.VersionMask, job);
            version = (job.Version & ~rollMask) | (submit.Version & rollMask);
        }

        // The submitted token selects the immutable target advertised with this work.
        var coinbasePrefix = session.GetCoinbasePrefix(job);
        var normalizedEn2 = en2[..expectedBytes];
        Span<byte> merkleRoot = stackalloc byte[32];
        if (!session.TryGetMerkleRoot(job.JobId, normalizedEn2, merkleRoot))
        {
            ShareValidator.ComputeMerkleRoot(job, coinbasePrefix, normalizedEn2, merkleRoot);
            session.SetMerkleRoot(job.JobId, normalizedEn2, merkleRoot);
        }

        return ShareValidator.ValidateWithMerkleRoot(
            job,
            coinbasePrefix,
            normalizedEn2,
            merkleRoot,
            submit.Ntime,
            submit.Nonce,
            version,
            shareTargetLe);
    }

    private static string ValidationFailureName(ShareValidationFailure failure) => failure switch
    {
        ShareValidationFailure.Extranonce2TooLong => "extranonce2_too_long",
        ShareValidationFailure.InvalidExtranonce2 => "invalid_extranonce2",
        ShareValidationFailure.InvalidNtime => "invalid_ntime",
        ShareValidationFailure.InvalidNonce => "invalid_nonce",
        ShareValidationFailure.NtimeBeforeMintime => "ntime_before_mintime",
        ShareValidationFailure.NtimeTooFarInFuture => "ntime_too_far_in_future",
        _ => "validation_failed"
    };

    private static void LogRejectedShare(
        ClientSession session,
        ParsedSubmit submit,
        string reason,
        JobTemplate? job = null,
        WorkAssignment? assignment = null,
        ShareResult? validation = null)
    {
        if (SoloLog.MinLevel > SoloLogLevel.Warning)
            return;

        var extranonce2 = "invalid";
        if (submit.Extranonce2Valid)
        {
            var width = Math.Clamp(submit.Extranonce2HexLength, 1, 16);
            extranonce2 = submit.Extranonce2.ToString($"x{width}", CultureInfo.InvariantCulture);
        }

        var submittedVersion = submit.HasVersion
            ? submit.VersionValid ? submit.Version.ToString("x8") : "invalid"
            : "none";
        var effectiveVersionMask = job != null && session.VersionRolling
            ? EffectiveVersionMask(session.VersionMask, job).ToString("x8")
            : "";
        var shareTarget = assignment.HasValue
            ? Hash256.FromLittleEndian(assignment.Value.Target.LittleEndian).ToHex()
            : "";
        var shareHash = "";
        double? actualDifficulty = null;
        if (validation is { HashComputed: true } computed)
        {
            shareHash = computed.Hash.ToHex();
            if (computed.ActualDiff > 0)
            {
                actualDifficulty = computed.ActualDiff;
            }
            else
            {
                Span<byte> hashLe = stackalloc byte[32];
                computed.Hash.WriteLittleEndian(hashLe);
                actualDifficulty = BitcoinEncoding.HashToDisplayDiff(hashLe);
            }
        }

        SoloLog.Warn("share rejected",
            ("reason", reason),
            ("peer", session.RemoteEndpoint),
            ("en1", session.Extranonce1Hex),
            ("ua", session.UserAgent),
            ("subscribed", session.Subscribed),
            ("authorized", session.Authorized),
            ("job_token", submit.JobKey == 0 ? "" : submit.JobKey.ToString("x")),
            ("job_id", job?.JobId ?? (submit.JobKey == 0 ? "" : submit.JobKey.ToString("x"))),
            ("template_job", job?.JobId ?? ""),
            ("height", job?.Height),
            ("en2", extranonce2),
            ("en2_hex_length", submit.Extranonce2HexLength),
            ("ntime", submit.NtimeValid ? submit.Ntime.ToString("x8") : "invalid"),
            ("nonce", submit.NonceValid ? submit.Nonce.ToString("x8") : "invalid"),
            ("version_bits", submittedVersion),
            ("effective_version_mask", effectiveVersionMask),
            ("assigned_diff", assignment?.Difficulty),
            ("share_hash", shareHash),
            ("share_target", shareTarget),
            ("actual_diff", actualDifficulty));
    }

    /// <summary>
    /// VarDiff with two triggers:
    /// - Steady watchdog: consume every elapsed window, including zero-share windows.
    /// - Burst submit path: retarget early only after retarget_share_burst observations.
    /// Work is difficulty-weighted so shares from older assignments cannot compound an increase.
    /// </summary>
    private async Task RetargetAsync(
        ClientSession session,
        bool allowBurst,
        CancellationToken ct)
    {
        await session.VarDiffLock.WaitAsync(ct);
        try
        {
            await RetargetLockedAsync(session, allowBurst);
        }
        finally
        {
            session.VarDiffLock.Release();
        }
    }

    /// <summary>Caller holds <see cref="ClientSession.VarDiffLock"/>.</summary>
    private Task RetargetLockedAsync(ClientSession session, bool allowBurst)
    {
        // Shares still arrive at the current assignment target until a public template
        // consumes the queued decision. Re-evaluating them would compound one retarget.
        if (session.PendingDifficulty.HasValue)
            return Task.CompletedTask;

        var now = DateTimeOffset.UtcNow;
        var elapsed = Math.Max(0.05, (now - session.LastRetarget).TotalSeconds);
        var shares = session.ShareCount;
        var work = session.AccumulatedWork;
        var previous = session.Difficulty;
        var decision = VarDiffCalculator.Evaluate(
            _cfg.Difficulty,
            previous,
            shares,
            work,
            elapsed,
            allowBurst,
            session.SmoothedDifficultyRatio);

        if (!decision.ResetWindow)
            return Task.CompletedTask;

        session.ResetVarDiffWindow(now);
        session.SmoothedDifficultyRatio = decision.SmoothedRatio;
        if (!decision.ApplyDifficulty)
            return Task.CompletedTask;

        session.SetPendingDifficulty(decision.NextDifficulty);
        if (SoloLog.MinLevel <= SoloLogLevel.Debug)
        {
            SoloLog.Debug("vardiff retarget",
                ("peer", session.RemoteEndpoint),
                ("en1", session.Extranonce1Hex),
                ("trigger", decision.BurstUp ? "burst" : shares == 0 ? "silence" : "window"),
                ("shares", shares),
                ("work", work.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
                ("elapsed_secs", elapsed.ToString("F3", System.Globalization.CultureInfo.InvariantCulture)),
                ("old_diff", previous.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
                ("pending_diff", decision.NextDifficulty.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)));
        }
        return Task.CompletedTask;
    }

    internal static bool HasValidVersionParameterShape(
        bool versionRolling,
        bool hasVersion,
        bool versionValid) =>
        versionRolling == hasVersion && (!hasVersion || versionValid);

    internal static bool TryParseVersionRollingMask(string? text, out uint value)
    {
        value = 0;
        return text is { Length: 8 } &&
            BitcoinEncoding.IsExactHex(text.AsSpan()) &&
            uint.TryParse(
                text.AsSpan(), NumberStyles.AllowHexSpecifier,
                CultureInfo.InvariantCulture, out value);
    }

    internal static bool AreSubmittedVersionBitsValid(
        uint versionBits,
        uint configuredMask,
        JobTemplate job) =>
        (versionBits & ~EffectiveVersionMask(configuredMask, job)) == 0;

    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        var reader = new SequenceReader<byte>(buffer);
        if (!reader.TryReadTo(out line, (byte)'\n', advancePastDelimiter: true))
            return false;

        buffer = buffer.Slice(reader.Position);
        if (!line.IsEmpty && line.Slice(line.Length - 1, 1).FirstSpan[0] == (byte)'\r')
            line = line.Slice(0, line.Length - 1);
        return true;
    }

    private static bool IsWhiteSpace(in ReadOnlySequence<byte> value)
    {
        foreach (var segment in value)
        {
            foreach (var b in segment.Span)
            {
                if (b is not ((byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n'))
                    return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Consume a queued decision only while registering the next public template. This
    /// binds set_difficulty, its immutable assignment target and mining.notify together.
    /// </summary>
    private byte[]? ApplyPendingDifficultyForJob(ClientSession session, JobTemplate job)
    {
        if (!session.TryTakePendingDifficulty(out var requested))
            return ClampSessionDifficultyForJob(session, job);

        var next = ClampDifficultyForJob(_cfg.Difficulty, requested, job);
        var nextTarget = ShareTarget.FromDifficulty(next);
        if (Math.Abs(next - session.Difficulty) / Math.Max(session.Difficulty, 1e-12) < 1e-9 &&
            nextTarget.ValueEquals(session.Target))
        {
            return null;
        }

        var previous = session.Difficulty;
        session.ResetDifficulty(next, DateTimeOffset.UtcNow);
        _metrics.TouchWorker(ToWorkerIdentity(session), session.Difficulty);
        if (SoloLog.MinLevel <= SoloLogLevel.Debug)
        {
            SoloLog.Debug("pending difficulty applied",
                ("peer", session.RemoteEndpoint),
                ("en1", session.Extranonce1Hex),
                ("height", job.Height),
                ("old_diff", previous.ToString("G6", CultureInfo.InvariantCulture)),
                ("requested_diff", requested.ToString("G6", CultureInfo.InvariantCulture)),
                ("new_diff", next.ToString("G6", CultureInfo.InvariantCulture)));
        }
        return BuildSetDifficultyBytes(next);
    }

    private byte[]? ClampSessionDifficultyForJob(ClientSession session, JobTemplate job)
    {
        // Caller holds VarDiffLock through assignment registration and queueing.
        if (!RequiresDownwardDifficultyClampForJob(
                _cfg.Difficulty,
                session.Difficulty,
                session.ShareTargetLe,
                job))
            return null;

        var next = ClampDifficultyForJob(_cfg.Difficulty, session.Difficulty, job);
        if (next >= session.Difficulty ||
            Math.Abs(next - session.Difficulty) / Math.Max(session.Difficulty, 1e-12) < 1e-9)
            return null;

        var previous = session.Difficulty;
        session.ResetDifficulty(next, DateTimeOffset.UtcNow);
        _metrics.TouchWorker(ToWorkerIdentity(session), session.Difficulty);
        SoloLog.Info("share difficulty clamped to network target",
            ("peer", session.RemoteEndpoint),
            ("en1", session.Extranonce1Hex),
            ("height", job.Height),
            ("old_diff", previous.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
            ("new_diff", next.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)),
            ("network_diff", job.NetworkDifficulty.ToString("G6", System.Globalization.CultureInfo.InvariantCulture)));
        return BuildSetDifficultyBytes(next);
    }

    internal static bool RequiresDownwardDifficultyClampForJob(
        DifficultyConfig config,
        double currentDifficulty,
        ReadOnlySpan<byte> currentShareTargetLe,
        JobTemplate job)
    {
        if (!double.IsFinite(currentDifficulty) || currentDifficulty <= 0 ||
            currentDifficulty > config.Max)
            return true;

        var hasNetworkLimit = job.Ready &&
            double.IsFinite(job.NetworkDifficulty) && job.NetworkDifficulty > 0;
        if (!hasNetworkLimit)
            return false;

        // The session already caches the exact integer target corresponding to its
        // advertised difficulty. Comparing it with nbits avoids BigInteger conversion
        // and allocation for the normal case where the share target is already easier.
        return job.TargetLe.Length != 32 || currentShareTargetLe.Length != 32 ||
            !BitcoinEncoding.LeqLe256(job.TargetLe, currentShareTargetLe);
    }

    internal static double ClampDifficultyForJob(
        DifficultyConfig config,
        double requested,
        JobTemplate job)
    {
        if (!double.IsFinite(requested) || requested <= 0)
            requested = config.Default;

        var maximum = config.Max;
        var hasNetworkLimit = job.Ready &&
            double.IsFinite(job.NetworkDifficulty) && job.NetworkDifficulty > 0;
        if (hasNetworkLimit)
            maximum = Math.Min(maximum, job.NetworkDifficulty * NetworkDifficultySafetyFactor);

        if (!double.IsFinite(maximum) || maximum <= 0)
            maximum = config.Min;
        var minimum = Math.Min(config.Min, maximum);
        var difficulty = Math.Clamp(requested, minimum, maximum);

        if (!hasNetworkLimit)
            return difficulty;

        // The floating-point display difficulty is derived from compact nbits. Verify
        // the resulting integer target so rounding can never make it harder than nbits.
        for (var i = 0; i < 64; i++)
        {
            var shareTarget = BitcoinEncoding.DiffToShareTargetLe(difficulty);
            if (BitcoinEncoding.LeqLe256(job.TargetLe, shareTarget))
                return difficulty;
            difficulty *= 0.5;
        }

        throw new InvalidOperationException("could not derive a share target below the network difficulty");
    }

    private static void QueueAssignedJob(
        ClientSession session,
        JobTemplate job,
        bool clean,
        byte[]? difficultyFrame = null)
    {
        session.RegisterWorkAssignment(
            job.JobKey, job, session.Difficulty, session.Target);

        var notifyFrame = BuildMiningNotifyBytes(job, clean);
        var cleanNotifyFrame = clean
            ? notifyFrame
            : BuildMiningNotifyBytes(job, clean: true);
        var versionFrame = session.VersionRolling
            ? BuildVersionMaskBytes(session, job)
            : null;
        if (!session.TryQueueJob(
                job.Epoch, clean, versionFrame,
                notifyFrame, cleanNotifyFrame, difficultyFrame))
            throw new IOException("stratum client writer is closed");
        session.MarkLastSentWork(job);
    }

    /// <summary>Shared mining.notify frame for broadcast fan-out and single-client push.</summary>
    private static byte[] BuildMiningNotifyBytes(
        JobTemplate job,
        bool clean)
    {
        using var buffer = new PooledByteBufferWriter(512 + job.MerkleBranchesHex.Count * 68);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteNull("id");
            json.WriteString("method", "mining.notify");
            json.WriteStartArray("params");
            json.WriteStringValue(job.JobId);
            json.WriteStringValue(job.PrevhashNotifyHex);
            json.WriteStringValue(job.Coinbase1Hex);
            json.WriteStringValue(job.Coinbase2Hex);
            json.WriteStartArray();
            foreach (var branch in job.MerkleBranchesHex)
                json.WriteStringValue(branch);
            json.WriteEndArray();
            json.WriteStringValue(job.VersionHex);
            json.WriteStringValue(job.NbitsHex);
            json.WriteStringValue(job.NtimeHex);
            json.WriteBooleanValue(clean);
            json.WriteEndArray();
            json.WriteEndObject();
        }
        return FinishFrame(buffer);
    }

    private static byte[] BuildSetDifficultyBytes(double difficulty)
    {
        using var buffer = new PooledByteBufferWriter(128);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteNull("id");
            json.WriteString("method", "mining.set_difficulty");
            json.WriteStartArray("params");
            json.WriteNumberValue(difficulty);
            json.WriteEndArray();
            json.WriteEndObject();
        }
        return FinishFrame(buffer);
    }

    private static byte[] BuildVersionMaskBytes(ClientSession session, JobTemplate job)
    {
        var mask = EffectiveVersionMask(session.VersionMask, job).ToString("x8");
        using var buffer = new PooledByteBufferWriter(128);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteNull("id");
            json.WriteString("method", "mining.set_version_mask");
            json.WriteStartArray("params");
            json.WriteStringValue(mask);
            json.WriteEndArray();
            json.WriteEndObject();
        }
        return FinishFrame(buffer);
    }

    internal static uint EffectiveVersionMask(uint configuredMask, JobTemplate job) =>
        configuredMask & ~job.Vbrequired;

    private static Dictionary<string, object?> Ok(JsonElement id, object? result) => new()
    {
        ["id"] = ToId(id),
        ["result"] = result,
        ["error"] = null
    };

    private static Dictionary<string, object?> Notify(string method, object parameters) => new()
    {
        ["id"] = null,
        ["method"] = method,
        ["params"] = parameters
    };

    private static object? ToId(JsonElement id) => id.ValueKind switch
    {
        JsonValueKind.Number => id.TryGetInt64(out var n) ? n : id.GetDouble(),
        JsonValueKind.String => id.GetString(),
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        _ => id.GetRawText()
    };

    /// <summary>Hot path: {"id":…,"result":true,"error":null} without Dictionary+Serialize.</summary>
    private static Task WriteOkTrueAsync(ClientSession session, JsonElement id)
    {
        WriteRaw(session, BuildOkTrueFrame(id));
        return Task.CompletedTask;
    }

    private static Task WriteAcceptedShareOkTrueAsync(
        ClientSession session,
        StratumRequestId id,
        long submitStartedTimestamp)
    {
        if (!session.TryQueueAcceptedShareResponse(BuildPooledOkTrueFrame(id), submitStartedTimestamp))
            throw new IOException("stratum client send queue is full or closed");
        return Task.CompletedTask;
    }

    private static byte[] BuildOkTrueFrame(JsonElement id)
    {
        using var buffer = new PooledByteBufferWriter(96);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WritePropertyName("id");
            WriteId(json, id);
            json.WriteBoolean("result", true);
            json.WriteNull("error");
            json.WriteEndObject();
        }
        return FinishFrame(buffer);
    }

    internal static PooledFrameBuffer BuildPooledOkTrueFrame(StratumRequestId id)
    {
        var writer = new PooledFrameWriter(96);
        try
        {
            writer.Write("{\"id\":"u8);
            WriteId(ref writer, id);
            writer.Write(",\"result\":true,\"error\":null}"u8);
            return writer.DetachWithNewline();
        }
        finally
        {
            writer.Dispose();
        }
    }

    /// <summary>Hot path stratum error without building intermediate dictionaries.</summary>
    private static Task WriteStratumErrorAsync(ClientSession session, JsonElement id, int code, string msg)
    {
        using var buffer = new PooledByteBufferWriter(160);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WritePropertyName("id");
            WriteId(json, id);
            json.WriteBoolean("result", false);
            json.WriteStartArray("error");
            json.WriteNumberValue(code);
            json.WriteStringValue(msg);
            json.WriteNullValue();
            json.WriteEndArray();
            json.WriteEndObject();
        }
        WriteRaw(session, FinishFrame(buffer));
        return Task.CompletedTask;
    }

    private static Task WriteStratumErrorAsync(
        ClientSession session,
        StratumRequestId id,
        int code,
        string msg)
    {
        if (!session.TryQueuePooledWrite(BuildPooledStratumErrorFrame(id, code, msg)))
            throw new IOException("stratum client send queue is full or closed");
        return Task.CompletedTask;
    }

    internal static PooledFrameBuffer BuildPooledStratumErrorFrame(
        StratumRequestId id,
        int code,
        string msg)
    {
        var writer = new PooledFrameWriter(160);
        try
        {
            writer.Write("{\"id\":"u8);
            WriteId(ref writer, id);
            writer.Write(",\"result\":false,\"error\":["u8);
            var numberBuffer = writer.GetSpan(16);
            if (!Utf8Formatter.TryFormat(code, numberBuffer, out var written))
                throw new InvalidOperationException("could not format Stratum error code");
            writer.Advance(written);
            writer.WriteByte((byte)',');
            WriteJsonString(ref writer, msg);
            writer.Write(",null]}"u8);
            return writer.DetachWithNewline();
        }
        finally
        {
            writer.Dispose();
        }
    }

    private static void WriteId(Utf8JsonWriter writer, JsonElement id)
    {
        if (id.ValueKind == JsonValueKind.Undefined)
            writer.WriteNullValue();
        else
            id.WriteTo(writer);
    }

    private static void WriteId(ref PooledFrameWriter writer, StratumRequestId id)
    {
        Span<byte> numberBuffer;
        int written;
        switch (id.Kind)
        {
            case StratumRequestIdKind.Int64:
                numberBuffer = writer.GetSpan(32);
                if (!Utf8Formatter.TryFormat(id.Signed, numberBuffer, out written))
                    throw new InvalidOperationException("could not format request id");
                writer.Advance(written);
                break;
            case StratumRequestIdKind.UInt64:
                numberBuffer = writer.GetSpan(32);
                if (!Utf8Formatter.TryFormat(id.Unsigned, numberBuffer, out written))
                    throw new InvalidOperationException("could not format request id");
                writer.Advance(written);
                break;
            case StratumRequestIdKind.Double:
                numberBuffer = writer.GetSpan(32);
                if (!Utf8Formatter.TryFormat(id.Floating, numberBuffer, out written))
                    throw new InvalidOperationException("could not format request id");
                writer.Advance(written);
                break;
            case StratumRequestIdKind.String:
                writer.WriteByte((byte)'\"');
                writer.Write(JsonEncodedText.Encode(id.Text ?? "").EncodedUtf8Bytes);
                writer.WriteByte((byte)'\"');
                break;
            case StratumRequestIdKind.RawJson:
                var raw = id.Text ?? "null";
                var byteCount = Encoding.UTF8.GetByteCount(raw);
                var rawDestination = writer.GetSpan(byteCount);
                writer.Advance(Encoding.UTF8.GetBytes(raw.AsSpan(), rawDestination));
                break;
            default:
                writer.Write("null"u8);
                break;
        }
    }

    private static void WriteJsonString(ref PooledFrameWriter writer, string value)
    {
        var asciiSafe = true;
        foreach (var c in value)
        {
            if (c is < (char)0x20 or > (char)0x7f or '\"' or '\\')
            {
                asciiSafe = false;
                break;
            }
        }

        writer.WriteByte((byte)'\"');
        if (asciiSafe)
        {
            var destination = writer.GetSpan(value.Length);
            for (var i = 0; i < value.Length; i++)
                destination[i] = (byte)value[i];
            writer.Advance(value.Length);
        }
        else
        {
            writer.Write(JsonEncodedText.Encode(value).EncodedUtf8Bytes);
        }
        writer.WriteByte((byte)'\"');
    }

    private static Task WriteAsync(ClientSession session, object payload)
    {
        using var buffer = new PooledByteBufferWriter(256);
        using (var json = new Utf8JsonWriter(buffer))
            JsonSerializer.Serialize(json, payload, payload.GetType());
        WriteRaw(session, FinishFrame(buffer));
        return Task.CompletedTask;
    }

    private static byte[] FinishFrame(PooledByteBufferWriter buffer)
    {
        var destination = new byte[buffer.WrittenCount + 1];
        buffer.WrittenSpan.CopyTo(destination);
        destination[^1] = (byte)'\n';
        return destination;
    }

    private static void WriteRaw(ClientSession session, byte[] bytes)
    {
        if (!session.TryQueueWrite(bytes))
            throw new IOException("stratum client send queue is full or closed");
    }

    private static void QueueDifficulty(ClientSession session, double difficulty)
    {
        if (!session.TryQueueDifficulty(BuildSetDifficultyBytes(difficulty)))
            throw new IOException("stratum client send queue is full or closed");
    }

    /// <summary>
    /// One Stratum TCP session = one metrics worker (session-scoped).
    /// Peer is for logs / internal metrics; public dashboard uses extranonce1 + user-agent only.
    /// </summary>
    private static WorkerIdentity ToWorkerIdentity(ClientSession session) => session.GetWorkerIdentity();

    private static string FormatPeer(TcpClient tcp) =>
        tcp.Client.RemoteEndPoint is IPEndPoint ip
            ? $"{ip.Address}:{ip.Port}"
            : tcp.Client.RemoteEndPoint?.ToString() ?? "unknown";
}

internal enum StratumRequestIdKind : byte
{
    Null,
    Int64,
    UInt64,
    Double,
    String,
    RawJson
}

internal readonly struct StratumRequestId
{
    private StratumRequestId(
        StratumRequestIdKind kind,
        long signed,
        ulong unsigned,
        double floating,
        string? text)
    {
        Kind = kind;
        Signed = signed;
        Unsigned = unsigned;
        Floating = floating;
        Text = text;
    }

    public static StratumRequestId Null => default;
    public static StratumRequestId FromInt64(long value) =>
        new(StratumRequestIdKind.Int64, value, 0, 0, null);
    public static StratumRequestId FromUInt64(ulong value) =>
        new(StratumRequestIdKind.UInt64, 0, value, 0, null);
    public static StratumRequestId FromDouble(double value) =>
        new(StratumRequestIdKind.Double, 0, 0, value, null);
    public static StratumRequestId FromString(string value) =>
        new(StratumRequestIdKind.String, 0, 0, 0, value);
    public static StratumRequestId FromRawJson(string value) =>
        new(StratumRequestIdKind.RawJson, 0, 0, 0, value);

    public StratumRequestIdKind Kind { get; }
    public long Signed { get; }
    public ulong Unsigned { get; }
    public double Floating { get; }
    public string? Text { get; }
}

internal readonly record struct ParsedSubmit(
    StratumRequestId Id,
    bool HasRequiredParams,
    ulong JobKey,
    ulong Extranonce2,
    int Extranonce2HexLength,
    bool Extranonce2Valid,
    uint Ntime,
    bool NtimeValid,
    uint Nonce,
    bool NonceValid,
    uint Version,
    bool HasVersion,
    bool VersionValid)
{
    public bool TryWriteExtranonce2(int expectedBytes, Span<byte> destination)
    {
        if (!Extranonce2Valid || expectedBytes is < 1 or > 8 || destination.Length < expectedBytes ||
            Extranonce2HexLength > expectedBytes * 2)
            return false;

        var value = Extranonce2;
        for (var i = expectedBytes - 1; i >= 0; i--)
        {
            destination[i] = (byte)value;
            value >>= 8;
        }
        return true;
    }
}

internal enum ShareValidationFailure
{
    None,
    Extranonce2TooLong,
    InvalidExtranonce2,
    InvalidNtime,
    InvalidNonce,
    NtimeBeforeMintime,
    NtimeTooFarInFuture
}

internal struct ParsedSubmitBuilder
{
    public StratumRequestId Id;
    public bool HasRequiredParams;
    public int ParamsCount;
    public bool ParamsTypesValid;
    public ulong JobKey;
    public ulong Extranonce2;
    public int Extranonce2HexLength;
    public bool Extranonce2Valid;
    public uint Ntime;
    public bool NtimeValid;
    public uint Nonce;
    public bool NonceValid;
    public uint Version;
    public bool HasVersion;
    public bool VersionValid;

    public readonly ParsedSubmit Build() => new(
        Id,
        HasRequiredParams,
        JobKey,
        Extranonce2,
        Extranonce2HexLength,
        Extranonce2Valid,
        Ntime,
        NtimeValid,
        Nonce,
        NonceValid,
        Version,
        HasVersion,
        VersionValid);
}

internal sealed class ClientSession : IDisposable
{
    public Guid Id { get; } = Guid.NewGuid();
    public string SessionId { get; }
    public TcpClient Tcp { get; }
    public NetworkStream Stream { get; }
    public string RemoteEndpoint { get; }
    public SemaphoreSlim VarDiffLock { get; } = new(1, 1);
    public bool Subscribed { get; set; }
    public bool Authorized { get; set; }
    public string UserAgent { get; set; } = "Unknown";
    public string? LastSentJobId { get; set; }
    public string Worker { get; set; } = "worker";
    public uint Extranonce1 { get; set; }
    public bool HasExtranonceLease { get; set; }
    public byte[] Extranonce1Bytes { get; set; } = Array.Empty<byte>();
    public string Extranonce1Hex { get; set; } = "";
    public double Difficulty { get; private set; }
    public ShareTarget Target { get; private set; }
    public ReadOnlySpan<byte> ShareTargetLe => Target.LittleEndian;
    public double? PendingDifficulty { get; private set; }
    public double BestDiff { get; set; }
    public bool VersionRolling { get; set; }
    public uint VersionMask { get; set; } = 0x1fffe000;
    public DateTimeOffset LastRetarget { get; set; } = DateTimeOffset.UtcNow;
    /// <summary>Updated on inbound lines while the handshake idle watchdog is relevant.</summary>
    public DateTimeOffset LastActivityUtc { get; set; } = DateTimeOffset.UtcNow;
    public int ShareCount { get; set; }
    public double AccumulatedWork { get; set; }
    /// <summary>Cross-window EWMA of estimated/current difficulty; 1 means on target.</summary>
    public double SmoothedDifficultyRatio { get; set; } = 1.0;
    public long AcceptedShares { get; set; }

    private string? _coinbasePrefixJobId;
    private byte[] _coinbasePrefix = Array.Empty<byte>();
    private ShareTarget? _lastSentShareTarget;
    private WorkerIdentity? _workerIdentity;
    private readonly Channel<OutboundFrame> _sendQueue;
    private readonly object _outboundLock = new();
    private readonly WriterWakeSignal _writerSignal = new();
    private OutboundFrame? _pendingJob;
    private readonly CancellationTokenSource _writerCts = new();
    private readonly CancellationTokenSource _writeTimeoutCts;
    private readonly Task _writerTask;
    private readonly TimeSpan _writeTimeout;
    private readonly MetricsStore? _metrics;
    private readonly AcceptedShareTracker _submittedShares = new();
    private readonly WorkAssignmentTracker _workAssignments = new();
    private readonly MerkleRootCache _merkleRootCache = new();
    private int _writerStopping;
    private long _outboundSequence;
    private long _lastQueuedDifficultySequence;
    private long _lastWrittenJobEpoch;

    public ClientSession(TcpClient tcp, AppConfig cfg, MetricsStore? metrics = null)
    {
        SessionId = Id.ToString("N");
        Tcp = tcp;
        Stream = tcp.GetStream();
        RemoteEndpoint = tcp.Client.RemoteEndPoint is IPEndPoint ip
            ? $"{ip.Address}:{ip.Port}"
            : tcp.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _writeTimeout = TimeSpan.FromSeconds(cfg.Stratum.WriteTimeoutSecs);
        _writeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_writerCts.Token);
        _metrics = metrics;
        _sendQueue = Channel.CreateBounded<OutboundFrame>(new BoundedChannelOptions(cfg.Stratum.SendQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            // Wait mode plus non-blocking TryWrite gives an unambiguous false when full.
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        });
        _writerTask = RunWriterAsync();
        Difficulty = cfg.Difficulty.Default;
        Target = ShareTarget.FromDifficulty(Difficulty);
        LastActivityUtc = DateTimeOffset.UtcNow;
    }

    public bool TryQueueWrite(byte[] frame) =>
        TryQueueWrite(frame, OutboundFrameKind.Regular);

    public bool TryQueueDifficulty(byte[] frame) =>
        TryQueueWrite(frame, OutboundFrameKind.Difficulty);

    private bool TryQueueWrite(byte[] frame, OutboundFrameKind kind)
    {
        if (frame.Length == 0 || Volatile.Read(ref _writerStopping) != 0)
            return false;
        lock (_outboundLock)
        {
            if (Volatile.Read(ref _writerStopping) != 0)
                return false;
            var sequence = NextOutboundSequence();
            var outbound = kind == OutboundFrameKind.Difficulty
                ? OutboundFrame.Difficulty(sequence, frame)
                : new OutboundFrame(sequence, frame);
            if (!_sendQueue.Writer.TryWrite(outbound))
                return false;
            if (kind == OutboundFrameKind.Difficulty)
                _lastQueuedDifficultySequence = sequence;
        }
        SignalWriter();
        return true;
    }

    public bool TryQueuePooledWrite(PooledFrameBuffer frame) =>
        TryQueuePooledWrite(frame, acceptedShareResponse: false, submitStartedTimestamp: 0);

    public bool TryQueueAcceptedShareResponse(
        PooledFrameBuffer frame,
        long submitStartedTimestamp) =>
        TryQueuePooledWrite(frame, acceptedShareResponse: true, submitStartedTimestamp);

    private bool TryQueuePooledWrite(
        PooledFrameBuffer frame,
        bool acceptedShareResponse,
        long submitStartedTimestamp)
    {
        if (frame.Length <= 0 || frame.Length > frame.Buffer.Length ||
            Volatile.Read(ref _writerStopping) != 0)
        {
            frame.Return();
            return false;
        }
        lock (_outboundLock)
        {
            if (Volatile.Read(ref _writerStopping) != 0)
            {
                frame.Return();
                return false;
            }
            var outbound = OutboundFrame.FromPooled(
                NextOutboundSequence(), frame, acceptedShareResponse, submitStartedTimestamp);
            if (!_sendQueue.Writer.TryWrite(outbound))
            {
                outbound.Release();
                return false;
            }
            if (acceptedShareResponse)
            {
                // The writer also takes _outboundLock before reading the channel, so
                // queued is recorded before written even on a loopback-fast socket.
                _metrics?.RecordAcceptedShareAckQueued(
                    Stopwatch.GetTimestamp() - submitStartedTimestamp);
            }
        }
        SignalWriter();
        return true;
    }

    public bool TryQueueJob(
        long epoch,
        bool cleanJobs,
        byte[]? versionFrame,
        byte[] notifyFrame,
        byte[] cleanNotifyFrame,
        byte[]? difficultyFrame = null)
    {
        if (epoch <= 0 || notifyFrame.Length == 0 || Volatile.Read(ref _writerStopping) != 0)
            return false;

        lock (_outboundLock)
        {
            if (Volatile.Read(ref _writerStopping) != 0)
                return false;
            _pendingJob = JobOutboundFrame.ReplacePending(
                _pendingJob,
                NextOutboundSequence,
                _lastQueuedDifficultySequence,
                epoch,
                cleanJobs,
                versionFrame,
                notifyFrame,
                cleanNotifyFrame,
                difficultyFrame);
        }
        SignalWriter();
        return true;
    }

    public long LastWrittenJobEpoch => Volatile.Read(ref _lastWrittenJobEpoch);
    public bool WriterUnavailable => Volatile.Read(ref _writerStopping) != 0 || _writerTask.IsCompleted;

    private long NextOutboundSequence() => Interlocked.Increment(ref _outboundSequence);

    private void SignalWriter()
    {
        _writerSignal.Signal();
    }

    private async Task RunWriterAsync()
    {
        try
        {
            while (!_writerCts.IsCancellationRequested)
            {
                var next = TryTakeNextOutbound();
                if (next == null)
                {
                    if (_sendQueue.Reader.Completion.IsCompleted && !HasPendingJob())
                        break;
                    await _writerSignal.WaitAsync(_writerCts.Token);
                    continue;
                }

                var outbound = next.Value;
                try
                {
                    if (outbound.IsJob)
                    {
                        if (outbound.DifficultyFrame is { Length: > 0 })
                            await WriteFrameAsync(outbound.DifficultyFrame);
                        if (outbound.VersionFrame is { Length: > 0 })
                            await WriteFrameAsync(outbound.VersionFrame);
                    }
                    await WriteFrameAsync(outbound.Memory);
                    if (outbound.IsAcceptedShareResponse)
                    {
                        _metrics?.RecordAcceptedShareAckWritten(
                            Stopwatch.GetTimestamp() - outbound.SubmitStartedTimestamp);
                    }
                    if (outbound.IsJob)
                        RecordWrittenJobEpoch(outbound.Epoch);
                }
                finally
                {
                    outbound.Release();
                }
            }
        }
        catch (OperationCanceledException) when (_writerCts.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            SoloLog.Warn("stratum client writer stopped",
                ("peer", RemoteEndpoint),
                ("en1", Extranonce1Hex),
                ("error", ex.Message));
            try { Tcp.Close(); } catch { }
        }
        finally
        {
            Interlocked.Exchange(ref _writerStopping, 1);
            _sendQueue.Writer.TryComplete();
            ReleasePendingFrames();
        }
    }

    private OutboundFrame? TryTakeNextOutbound()
    {
        lock (_outboundLock)
        {
            var hasRegular = _sendQueue.Reader.TryPeek(out var regular);
            var job = _pendingJob;
            if (!hasRegular && !job.HasValue)
                return null;
            if (job.HasValue && (!hasRegular || job.Value.Sequence < regular.Sequence))
            {
                _pendingJob = null;
                return job.Value;
            }

            return _sendQueue.Reader.TryRead(out var frame) ? frame : null;
        }
    }

    private bool HasPendingJob()
    {
        lock (_outboundLock)
            return _pendingJob != null;
    }

    private void ReleasePendingFrames()
    {
        lock (_outboundLock)
        {
            if (_pendingJob is { } pending)
            {
                pending.Release();
                _pendingJob = null;
            }
            while (_sendQueue.Reader.TryRead(out var queued))
                queued.Release();
        }
    }

    private void RecordWrittenJobEpoch(long epoch)
    {
        while (true)
        {
            var current = Volatile.Read(ref _lastWrittenJobEpoch);
            if (current >= epoch || Interlocked.CompareExchange(ref _lastWrittenJobEpoch, epoch, current) == current)
                return;
        }
    }

    private async Task WriteFrameAsync(ReadOnlyMemory<byte> frame)
    {
        _writeTimeoutCts.CancelAfter(_writeTimeout);
        try
        {
            await Stream.WriteAsync(frame, _writeTimeoutCts.Token);
        }
        catch (OperationCanceledException) when (!_writerCts.IsCancellationRequested)
        {
            SoloLog.Warn("stratum client write timeout",
                ("peer", RemoteEndpoint),
                ("en1", Extranonce1Hex),
                ("timeout_secs", _writeTimeout.TotalSeconds));
            throw new TimeoutException("stratum socket write timed out");
        }
        finally
        {
            if (!_writeTimeoutCts.IsCancellationRequested)
                _writeTimeoutCts.CancelAfter(Timeout.InfiniteTimeSpan);
        }
    }

    public async Task StopWriterAsync()
    {
        var initiatedStop = Interlocked.Exchange(ref _writerStopping, 1) == 0;
        if (initiatedStop)
        {
            _sendQueue.Writer.TryComplete();
            SignalWriter();
        }
        try
        {
            await _writerTask.WaitAsync(_writeTimeout);
        }
        catch
        {
            _writerCts.Cancel();
            try { Tcp.Close(); } catch { }
            try { await _writerTask; } catch { }
        }
    }

    public byte[] GetCoinbasePrefix(JobTemplate job)
    {
        if (string.Equals(_coinbasePrefixJobId, job.JobId, StringComparison.Ordinal))
            return _coinbasePrefix;

        var prefix = new byte[job.Coinbase1.Length + Extranonce1Bytes.Length];
        job.Coinbase1.CopyTo(prefix, 0);
        Extranonce1Bytes.CopyTo(prefix, job.Coinbase1.Length);
        _coinbasePrefix = prefix;
        _coinbasePrefixJobId = job.JobId;
        return prefix;
    }

    public WorkerIdentity GetWorkerIdentity()
    {
        return _workerIdentity ??= new WorkerIdentity
        {
            SessionId = SessionId,
            Name = string.IsNullOrWhiteSpace(Worker) ? "worker" : Worker.Trim(),
            UserAgent = string.IsNullOrWhiteSpace(UserAgent) ? "Unknown" : UserAgent.Trim(),
            Peer = RemoteEndpoint,
            Extranonce1 = Extranonce1Hex,
            IsNormalized = true
        };
    }

    public void ResetSubscriptionCaches()
    {
        LastSentJobId = null;
        _lastSentShareTarget = null;
        _coinbasePrefixJobId = null;
        _coinbasePrefix = Array.Empty<byte>();
        _merkleRootCache.Reset();
        ResetSubmittedShares();
        _workAssignments.Reset();
        InvalidateWorkerIdentity();
    }

    public bool LastSentWorkMatches(JobTemplate job) =>
        string.Equals(LastSentJobId, job.JobId, StringComparison.Ordinal) &&
        _lastSentShareTarget?.ValueEquals(Target) == true;

    public void MarkLastSentWork(JobTemplate job)
    {
        LastSentJobId = job.JobId;
        _lastSentShareTarget = Target;
    }

    public void DiscardPendingJob()
    {
        lock (_outboundLock)
        {
            if (_pendingJob is not { } pending)
                return;
            pending.Release();
            _pendingJob = null;
        }
    }

    public void RegisterWorkAssignment(
        ulong issuedJobKey,
        JobTemplate job,
        double difficulty,
        ShareTarget shareTarget) =>
        _workAssignments.Register(
            issuedJobKey, job.JobKey, job.Epoch, difficulty, shareTarget);

    public bool TryGetWorkAssignment(ulong issuedJobKey, out WorkAssignment assignment) =>
        _workAssignments.TryGet(issuedJobKey, out assignment);

    public bool TryGetRetiredWorkTemplateKey(ulong issuedJobKey, out ulong templateJobKey) =>
        _workAssignments.TryGetRetiredTemplateKey(issuedJobKey, out templateJobKey);

    public AcceptedShareRegistration TryRegisterSubmittedShare(
        long jobEpoch,
        Hash256 headerHash,
        bool isBlockCandidate = false) =>
        _submittedShares.TryRegister(jobEpoch, headerHash, isBlockCandidate);

    public void UnregisterSubmittedShare(long jobEpoch, Hash256 headerHash) =>
        _submittedShares.Remove(jobEpoch, headerHash);

    public void ResetSubmittedShares() => _submittedShares.Reset();

    public void PruneSubmittedSharesBefore(long minimumEpoch) =>
        _submittedShares.PruneBefore(minimumEpoch);

    public void PruneWorkAssignmentsBefore(long minimumEpoch) =>
        _workAssignments.PruneBefore(minimumEpoch);

    public bool TryGetMerkleRoot(
        string jobId,
        ReadOnlySpan<byte> extranonce2,
        Span<byte> destination) =>
        _merkleRootCache.TryGet(jobId, extranonce2, destination);

    public void SetMerkleRoot(string jobId, ReadOnlySpan<byte> extranonce2, ReadOnlySpan<byte> merkleRoot) =>
        _merkleRootCache.Set(jobId, extranonce2, merkleRoot);

    public void InvalidateWorkerIdentity() => _workerIdentity = null;

    public void ResetDifficulty(double difficulty, DateTimeOffset now)
    {
        Difficulty = difficulty;
        Target = ShareTarget.FromDifficulty(difficulty);
        PendingDifficulty = null;
        SmoothedDifficultyRatio = 1.0;
        ResetVarDiffWindow(now);
    }

    public void SetPendingDifficulty(double difficulty)
    {
        if (!double.IsFinite(difficulty) || difficulty <= 0)
            throw new ArgumentOutOfRangeException(nameof(difficulty));

        PendingDifficulty = Math.Abs(difficulty - Difficulty) /
            Math.Max(Difficulty, 1e-12) < 1e-9
            ? null
            : difficulty;
    }

    public bool TryTakePendingDifficulty(out double difficulty)
    {
        if (PendingDifficulty is not { } pending)
        {
            difficulty = 0;
            return false;
        }

        PendingDifficulty = null;
        difficulty = pending;
        return true;
    }

    public void ResetVarDiffWindow(DateTimeOffset now)
    {
        LastRetarget = now;
        ShareCount = 0;
        AccumulatedWork = 0;
    }

    public void RecordAcceptedShare(double creditDifficulty, double actualDifficulty)
    {
        ShareCount++;
        AccumulatedWork += Math.Max(0, creditDifficulty);
        BestDiff = Math.Max(BestDiff, actualDifficulty);
        AcceptedShares++;
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _writerStopping, 1);
        _sendQueue.Writer.TryComplete();
        _writerCts.Cancel();
        try { Stream.Dispose(); } catch { }
        try { Tcp.Dispose(); } catch { }
        _writeTimeoutCts.Dispose();
        _writerCts.Dispose();
        _writerSignal.Dispose();
    }
}

/// <summary>
/// Coalescing writer wake-up. The atomic latch guarantees that at most one semaphore
/// permit exists; the single consumer clears it only after consuming that permit.
/// </summary>
internal sealed class WriterWakeSignal : IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(0, 1);
    private int _latched;
    private int _disposed;

    public void Signal()
    {
        if (Volatile.Read(ref _disposed) != 0 || Volatile.Read(ref _latched) != 0 ||
            Interlocked.CompareExchange(ref _latched, 1, 0) != 0)
            return;

        try
        {
            _semaphore.Release();
        }
        catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0)
        {
            // A producer may have queued just before session shutdown and signal after
            // the writer has completed. The frame is already owned by shutdown cleanup.
        }
    }

    public async ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        Interlocked.Exchange(ref _latched, 0);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            _semaphore.Dispose();
    }
}

internal sealed class ExtranonceLeasePool
{
    private readonly object _gate = new();
    private readonly HashSet<uint> _leased = new();
    private readonly uint _mask;
    private readonly ulong _capacity;
    private uint _next;

    public ExtranonceLeasePool(int size)
    {
        if (size is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(size));
        var bits = size * 8;
        _mask = size == 4 ? uint.MaxValue : (1u << bits) - 1;
        _capacity = 1UL << bits;
    }

    public bool TryAcquire(out uint value)
    {
        lock (_gate)
        {
            if ((ulong)_leased.Count >= _capacity)
            {
                value = 0;
                return false;
            }

            // With N active leases, one of the next N+1 values must be free.
            for (long attempt = 0; attempt <= _leased.Count; attempt++)
            {
                _next = unchecked(_next + 1) & _mask;
                if (_leased.Add(_next))
                {
                    value = _next;
                    return true;
                }
            }
        }

        throw new InvalidOperationException("extranonce1 lease allocator invariant failed");
    }

    public bool Release(uint value)
    {
        lock (_gate)
            return _leased.Remove(value);
    }
}

internal readonly record struct PooledFrameBuffer(byte[] Buffer, int Length, bool IsTracked = false)
{
    internal static PooledFrameBuffer CreateTracked(byte[] buffer, int length)
    {
        Interlocked.Increment(ref PooledFrameOwnership.Outstanding);
        return new PooledFrameBuffer(buffer, length, IsTracked: true);
    }

    internal void Return()
    {
        ArrayPool<byte>.Shared.Return(Buffer);
        if (IsTracked)
            Interlocked.Decrement(ref PooledFrameOwnership.Outstanding);
    }
}

internal static class PooledFrameOwnership
{
    internal static long Outstanding;
}

internal ref struct PooledFrameWriter
{
    private byte[]? _buffer;
    private int _written;

    public PooledFrameWriter(int initialCapacity)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, initialCapacity));
        _written = 0;
    }

    public Span<byte> GetSpan(int sizeHint)
    {
        EnsureCapacity(sizeHint);
        return Buffer.AsSpan(_written);
    }

    public void Advance(int count)
    {
        if (count < 0 || count > Buffer.Length - _written)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public void Write(ReadOnlySpan<byte> value)
    {
        value.CopyTo(GetSpan(value.Length));
        Advance(value.Length);
    }

    public void WriteByte(byte value)
    {
        GetSpan(1)[0] = value;
        Advance(1);
    }

    public PooledFrameBuffer DetachWithNewline()
    {
        WriteByte((byte)'\n');
        var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledFrameWriter));
        _buffer = null;
        var result = PooledFrameBuffer.CreateTracked(buffer, _written);
        _written = 0;
        return result;
    }

    public void Dispose()
    {
        var buffer = _buffer;
        _buffer = null;
        _written = 0;
        if (buffer != null)
            ArrayPool<byte>.Shared.Return(buffer);
    }

    private byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(PooledFrameWriter));

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0)
            throw new ArgumentOutOfRangeException(nameof(sizeHint));
        if (sizeHint <= Buffer.Length - _written)
            return;

        var required = checked(_written + sizeHint);
        var replacement = ArrayPool<byte>.Shared.Rent(Math.Max(required, checked(Buffer.Length * 2)));
        Buffer.AsSpan(0, _written).CopyTo(replacement);
        ArrayPool<byte>.Shared.Return(Buffer);
        _buffer = replacement;
    }
}

internal enum OutboundFrameKind : byte
{
    Regular,
    Difficulty,
    Job,
    AcceptedShareResponse
}

internal readonly struct OutboundFrame
{
    public OutboundFrame(long sequence, byte[] frame)
        : this(sequence, frame, frame.Length, isPooled: false, OutboundFrameKind.Regular)
    {
    }

    private OutboundFrame(
        long sequence,
        byte[] frame,
        int length,
        bool isPooled,
        OutboundFrameKind kind,
        long epoch = 0,
        bool cleanJobs = false,
        byte[]? versionFrame = null,
        byte[]? difficultyFrame = null,
        long submitStartedTimestamp = 0)
    {
        Sequence = sequence;
        Frame = frame;
        Length = length;
        IsPooled = isPooled;
        Kind = kind;
        Epoch = epoch;
        CleanJobs = cleanJobs;
        VersionFrame = versionFrame;
        DifficultyFrame = difficultyFrame;
        SubmitStartedTimestamp = submitStartedTimestamp;
        PooledBufferTracked = false;
    }

    public long Sequence { get; }
    public byte[] Frame { get; }
    public int Length { get; }
    public bool IsPooled { get; }
    public OutboundFrameKind Kind { get; }
    public long Epoch { get; }
    public bool CleanJobs { get; }
    public byte[]? VersionFrame { get; }
    public byte[]? DifficultyFrame { get; }
    public long SubmitStartedTimestamp { get; }
    private bool PooledBufferTracked { get; init; }
    public ReadOnlyMemory<byte> Memory => Frame.AsMemory(0, Length);
    public bool IsJob => Kind == OutboundFrameKind.Job;
    public bool IsAcceptedShareResponse => Kind == OutboundFrameKind.AcceptedShareResponse;

    internal static long OutstandingPooledBufferCount =>
        Interlocked.Read(ref PooledFrameOwnership.Outstanding);

    internal static OutboundFrame FromPooled(
        long sequence,
        PooledFrameBuffer frame,
        bool acceptedShareResponse,
        long submitStartedTimestamp = 0) =>
        new OutboundFrame(
            sequence,
            frame.Buffer,
            frame.Length,
            isPooled: true,
            acceptedShareResponse
                ? OutboundFrameKind.AcceptedShareResponse
                : OutboundFrameKind.Regular,
            submitStartedTimestamp: submitStartedTimestamp)
        {
            PooledBufferTracked = frame.IsTracked
        };

    internal static OutboundFrame Difficulty(long sequence, byte[] frame) =>
        new(
            sequence,
            frame,
            frame.Length,
            isPooled: false,
            OutboundFrameKind.Difficulty);

    internal static OutboundFrame Job(
        long sequence,
        long epoch,
        bool cleanJobs,
        byte[]? versionFrame,
        byte[] notifyFrame,
        byte[]? difficultyFrame) =>
        new(
            sequence,
            notifyFrame,
            notifyFrame.Length,
            isPooled: false,
            OutboundFrameKind.Job,
            epoch,
            cleanJobs,
            versionFrame,
            difficultyFrame);

    internal void Release()
    {
        if (!IsPooled)
            return;
        ArrayPool<byte>.Shared.Return(Frame);
        if (PooledBufferTracked)
            Interlocked.Decrement(ref PooledFrameOwnership.Outstanding);
    }
}

internal static class JobOutboundFrame
{
    internal static OutboundFrame ReplacePending(
        OutboundFrame? pending,
        Func<long> nextSequence,
        long lastQueuedDifficultySequence,
        long epoch,
        bool cleanJobs,
        byte[]? versionFrame,
        byte[] notifyFrame,
        byte[] cleanNotifyFrame,
        byte[]? difficultyFrame = null)
    {
        var clean = cleanJobs || (pending?.CleanJobs ?? false);
        // A replacement normally occupies the original job slot so ordinary
        // responses cannot overtake a coalesced clean publication. A standalone
        // difficulty queued after that slot is an ordering barrier: the replacement
        // was registered at the new target and must follow the difficulty frame.
        var crossesDifficultyBarrier = pending.HasValue &&
            pending.Value.Sequence < lastQueuedDifficultySequence;
        var sequence = pending.HasValue && !crossesDifficultyBarrier
            ? pending.Value.Sequence
            : nextSequence();
        return OutboundFrame.Job(
            sequence,
            epoch,
            clean,
            versionFrame,
            clean ? cleanNotifyFrame : notifyFrame,
            difficultyFrame ?? (crossesDifficultyBarrier ? null : pending?.DifficultyFrame));
    }
}

internal enum AcceptedShareRegistration
{
    Added,
    Duplicate,
    CapacityExceeded
}

/// <summary>
/// Immutable 256-bit little-endian share target. A session and every work token issued
/// at the same difficulty share this object; changing difficulty creates a new object.
/// </summary>
internal sealed class ShareTarget
{
    private readonly byte[] _littleEndian;

    private ShareTarget(byte[] littleEndian)
    {
        if (littleEndian.Length != 32)
            throw new ArgumentException("share target must be 32 bytes", nameof(littleEndian));
        _littleEndian = littleEndian;
    }

    public ReadOnlySpan<byte> LittleEndian => _littleEndian;

    public static ShareTarget FromDifficulty(double difficulty) =>
        new(BitcoinEncoding.DiffToShareTargetLe(difficulty));

    public static ShareTarget CopyFrom(ReadOnlySpan<byte> littleEndian)
    {
        if (littleEndian.Length != 32)
            throw new ArgumentException("share target must be 32 bytes", nameof(littleEndian));
        return new ShareTarget(littleEndian.ToArray());
    }

    public bool ValueEquals(ShareTarget other) =>
        ReferenceEquals(this, other) || _littleEndian.AsSpan().SequenceEqual(other._littleEndian);

    public bool ValueEquals(ReadOnlySpan<byte> other) =>
        _littleEndian.AsSpan().SequenceEqual(other);

    public byte[] ToArray() => _littleEndian.ToArray();
}

internal readonly record struct WorkAssignment(
    ulong TemplateJobKey,
    long TemplateEpoch,
    double Difficulty,
    ShareTarget Target)
{
    // Compatibility/debug view. Production validation reads Target.LittleEndian directly.
    public byte[] ShareTargetLe => Target.ToArray();
}

/// <summary>
/// Per-connection Stratum public-job assignments. A job target is immutable once advertised;
/// pending VarDiff changes bind to the next public template.
/// </summary>
internal sealed class WorkAssignmentTracker
{
    internal const int Capacity = 256;
    private readonly object _gate = new();
    private readonly Dictionary<ulong, WorkAssignment> _assignments = new();
    private readonly Queue<ulong> _insertionOrder = new();
    private readonly Dictionary<ulong, ulong> _retiredTemplateKeys = new();
    private readonly Queue<(ulong IssuedJobKey, ulong TemplateJobKey)> _retiredInsertionOrder = new();

    internal int Count
    {
        get { lock (_gate) return _assignments.Count; }
    }

    public void Register(
        ulong issuedJobKey,
        ulong templateJobKey,
        long templateEpoch,
        double difficulty,
        ReadOnlySpan<byte> shareTargetLe)
    {
        Register(
            issuedJobKey,
            templateJobKey,
            templateEpoch,
            difficulty,
            ShareTarget.CopyFrom(shareTargetLe));
    }

    public void Register(
        ulong issuedJobKey,
        ulong templateJobKey,
        long templateEpoch,
        double difficulty,
        ShareTarget shareTarget)
    {
        if (issuedJobKey == 0 || templateJobKey == 0 || templateEpoch < 0 ||
            !double.IsFinite(difficulty) || difficulty <= 0 || shareTarget == null)
            throw new ArgumentOutOfRangeException(nameof(issuedJobKey), "invalid work assignment");

        lock (_gate)
            RegisterLocked(issuedJobKey, templateJobKey, templateEpoch, difficulty, shareTarget);
    }

    public bool TryGet(ulong issuedJobKey, out WorkAssignment assignment)
    {
        lock (_gate)
        {
            if (_assignments.TryGetValue(issuedJobKey, out assignment))
            {
                return true;
            }
        }

        assignment = default;
        return false;
    }

    public bool TryGetRetiredTemplateKey(ulong issuedJobKey, out ulong templateJobKey)
    {
        lock (_gate)
            return _retiredTemplateKeys.TryGetValue(issuedJobKey, out templateJobKey);
    }

    public void Reset()
    {
        lock (_gate)
        {
            _assignments.Clear();
            _insertionOrder.Clear();
            _retiredTemplateKeys.Clear();
            _retiredInsertionOrder.Clear();
        }
    }

    public void PruneBefore(long minimumEpoch)
    {
        lock (_gate)
        {
            var queued = _insertionOrder.Count;
            for (var i = 0; i < queued; i++)
            {
                var issuedJobKey = _insertionOrder.Dequeue();
                if (!_assignments.TryGetValue(issuedJobKey, out var assignment))
                    continue;
                if (assignment.TemplateEpoch < minimumEpoch)
                {
                    _assignments.Remove(issuedJobKey);
                    RememberRetiredLocked(issuedJobKey, assignment.TemplateJobKey);
                    continue;
                }
                _insertionOrder.Enqueue(issuedJobKey);
            }
        }
    }

    private void RegisterLocked(
        ulong issuedJobKey,
        ulong templateJobKey,
        long templateEpoch,
        double difficulty,
        ShareTarget shareTarget)
    {
        // A duplicate registration is valid only when every immutable field agrees.
        // Silently keeping a different target would make the advertised difficulty and
        // validation target diverge for the lifetime of this token.
        if (_assignments.TryGetValue(issuedJobKey, out var existing))
        {
            if (existing.TemplateJobKey != templateJobKey ||
                existing.TemplateEpoch != templateEpoch ||
                !existing.Difficulty.Equals(difficulty) ||
                !existing.Target.ValueEquals(shareTarget))
            {
                throw new InvalidOperationException(
                    $"work token {issuedJobKey:x} was registered with conflicting assignment data");
            }
            return;
        }

        if (_assignments.Count >= Capacity)
            throw new InvalidOperationException("work assignment capacity reached");

        _retiredTemplateKeys.Remove(issuedJobKey);
        var assignment = new WorkAssignment(
            templateJobKey, templateEpoch, difficulty, shareTarget);
        _assignments.Add(issuedJobKey, assignment);
        _insertionOrder.Enqueue(issuedJobKey);
    }

    private void RememberRetiredLocked(ulong issuedJobKey, ulong templateJobKey)
    {
        if (!_retiredTemplateKeys.TryAdd(issuedJobKey, templateJobKey))
            return;

        _retiredInsertionOrder.Enqueue((issuedJobKey, templateJobKey));
        while (_retiredTemplateKeys.Count > Capacity &&
               _retiredInsertionOrder.TryDequeue(out var oldest))
        {
            if (_retiredTemplateKeys.TryGetValue(oldest.IssuedJobKey, out var current) &&
                current == oldest.TemplateJobKey)
                _retiredTemplateKeys.Remove(oldest.IssuedJobKey);
        }
    }
}

internal sealed class AcceptedShareTracker
{
    internal const int Capacity = 8192;

    private readonly object _gate = new();
    private readonly Dictionary<long, HashSet<Hash256>> _headerHashesByEpoch = new();
    private int _count;

    public AcceptedShareRegistration TryRegister(
        long jobEpoch,
        Hash256 headerHash,
        bool isBlockCandidate = false)
    {
        if (jobEpoch < 0)
            throw new ArgumentOutOfRangeException(nameof(jobEpoch));
        lock (_gate)
        {
            if (_headerHashesByEpoch.TryGetValue(jobEpoch, out var hashes) &&
                hashes.Contains(headerHash))
                return AcceptedShareRegistration.Duplicate;
            // Capacity protects the ordinary-share telemetry path. A header that already
            // satisfies the network target must still acquire duplicate ownership and
            // reach submitblock; producing such entries requires real network PoW.
            if (_count >= Capacity && !isBlockCandidate)
                return AcceptedShareRegistration.CapacityExceeded;

            if (hashes == null)
            {
                hashes = new HashSet<Hash256>();
                _headerHashesByEpoch.Add(jobEpoch, hashes);
            }
            hashes.Add(headerHash);
            _count++;
            return AcceptedShareRegistration.Added;
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            _headerHashesByEpoch.Clear();
            _count = 0;
        }
    }

    public void Remove(long jobEpoch, Hash256 headerHash)
    {
        lock (_gate)
        {
            if (!_headerHashesByEpoch.TryGetValue(jobEpoch, out var hashes) || !hashes.Remove(headerHash))
                return;
            _count--;
            if (hashes.Count == 0)
                _headerHashesByEpoch.Remove(jobEpoch);
        }
    }

    public void PruneBefore(long minimumEpoch)
    {
        lock (_gate)
        {
            List<long>? expiredEpochs = null;
            foreach (var epoch in _headerHashesByEpoch.Keys)
            {
                if (epoch < minimumEpoch)
                    (expiredEpochs ??= new List<long>()).Add(epoch);
            }

            if (expiredEpochs == null)
                return;

            foreach (var epoch in expiredEpochs)
            {
                _count -= _headerHashesByEpoch[epoch].Count;
                _headerHashesByEpoch.Remove(epoch);
            }
            if (_count < 0)
                _count = 0;
        }
    }
}

internal sealed class MerkleRootCache
{
    internal const int Capacity = 8;

    // ClientSession serializes cache access with VarDiffLock. Keeping the cache itself
    // lock-free avoids a second monitor on every share while retaining fixed storage.
    private readonly CacheEntry[] _entries = new CacheEntry[Capacity];
    private int _nextIndex;

    public bool TryGet(
        string jobId,
        ReadOnlySpan<byte> extranonce2,
        Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("destination must be at least 32 bytes", nameof(destination));
        var key = Pack(extranonce2);
        for (var offset = 0; offset < Capacity; offset++)
        {
            var index = (_nextIndex - 1 - offset) & (Capacity - 1);
            ref readonly var entry = ref _entries[index];
            if (entry.Occupied &&
                entry.Extranonce2Length == extranonce2.Length &&
                entry.Extranonce2 == key &&
                string.Equals(entry.JobId, jobId, StringComparison.Ordinal))
            {
                entry.MerkleRoot.WriteLittleEndian(destination);
                return true;
            }
        }

        return false;
    }

    public void Set(string jobId, ReadOnlySpan<byte> extranonce2, ReadOnlySpan<byte> merkleRoot)
    {
        if (merkleRoot.Length != 32)
            throw new ArgumentException("merkle root must be 32 bytes", nameof(merkleRoot));
        var key = Pack(extranonce2);

        for (var offset = 0; offset < Capacity; offset++)
        {
            var index = (_nextIndex - 1 - offset) & (Capacity - 1);
            ref var existing = ref _entries[index];
            if (!existing.Occupied ||
                existing.Extranonce2Length != extranonce2.Length ||
                existing.Extranonce2 != key ||
                !string.Equals(existing.JobId, jobId, StringComparison.Ordinal))
                continue;

            existing.MerkleRoot = Hash256.FromLittleEndian(merkleRoot);
            return;
        }

        _entries[_nextIndex] = new CacheEntry
        {
            JobId = jobId,
            Extranonce2 = key,
            Extranonce2Length = extranonce2.Length,
            MerkleRoot = Hash256.FromLittleEndian(merkleRoot),
            Occupied = true
        };
        _nextIndex = (_nextIndex + 1) & (Capacity - 1);
    }

    public void Reset()
    {
        Array.Clear(_entries);
        _nextIndex = 0;
    }

    private static ulong Pack(ReadOnlySpan<byte> value)
    {
        if (value.Length is < 1 or > 8)
            throw new ArgumentOutOfRangeException(nameof(value), "extranonce2 must be 1 to 8 bytes");
        ulong result = 0;
        for (var i = 0; i < value.Length; i++)
            result = (result << 8) | value[i];
        return result;
    }

    private struct CacheEntry
    {
        public string? JobId;
        public ulong Extranonce2;
        public int Extranonce2Length;
        public Hash256 MerkleRoot;
        public bool Occupied;
    }
}

internal sealed class CleanBroadcastBarrier
{
    public CleanBroadcastBarrier(long epoch, ClientSession[] clients, DateTimeOffset deadline)
    {
        Epoch = epoch;
        Clients = clients;
        Deadline = deadline;
    }

    public long Epoch { get; }
    public ClientSession[] Clients { get; }
    public DateTimeOffset Deadline { get; }
}
