using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Rpc;

public sealed class BitcoinRpcClient : IDisposable
{
    private readonly HttpClient _client;
    private readonly HttpClient _longpollClient;
    private readonly SocketsHttpHandler _handler;
    private readonly string _url;
    private readonly AuthenticationHeaderValue _auth;
    private static long _id;

    /// <param name="requestTimeoutSecs">
    /// Timeout for ordinary RPC (incl. full GBT). Large mempools need more than 15s.
    /// </param>
    /// <param name="longpollTimeoutSecs">
    /// Timeout for longpoll GBT (bitcoind holds the request open ~1 min).
    /// </param>
    public BitcoinRpcClient(
        string url,
        string user,
        string pass,
        int requestTimeoutSecs = 60,
        int longpollTimeoutSecs = 130)
    {
        _url = url.TrimEnd('/');
        _auth = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{user}:{pass}")));

        // Shared handler: recycle connections so half-closed keep-alives from bitcoind
        // (common under concurrent GBT / longpoll) do not surface as send failures.
        _handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(30),
            MaxConnectionsPerServer = 8,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(10)
        };

        _client = new HttpClient(_handler, disposeHandler: false)
        {
            Timeout = TimeSpan.FromSeconds(Math.Clamp(requestTimeoutSecs, 15, 300))
        };
        _longpollClient = new HttpClient(_handler, disposeHandler: false)
        {
            Timeout = TimeSpan.FromSeconds(Math.Clamp(longpollTimeoutSecs, 60, 300))
        };
    }

    public Task<T> CallAsync<T>(string method, object? parameters = null, CancellationToken ct = default)
        => CallWithRetryAsync<T>(_client, method, parameters, maxAttempts: 3, ct);

    public Task<T> CallLongpollAsync<T>(string method, object? parameters = null, CancellationToken ct = default)
        => CallInternalAsync<T>(_longpollClient, method, parameters, ct);

    public Task<string?> SubmitBlockAsync(string blockHex, CancellationToken ct = default) =>
        SubmitBlockCoreAsync(
            SubmitBlockRpcContent.Create(Interlocked.Increment(ref _id), blockHex), ct);

    public Task<string?> SubmitBlockAsync(BlockCandidate block, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(block);
        return SubmitBlockCoreAsync(
            SubmitBlockRpcContent.Create(Interlocked.Increment(ref _id), block.Bytes), ct);
    }

    private async Task<string?> SubmitBlockCoreAsync(HttpContent content, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, _url);
        req.Headers.Authorization = _auth;
        req.Content = content;

        using var resp = await SendWithDetailAsync(_client, req, "submitblock", ct);
        resp.EnsureSuccessStatusCode();
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        if (doc.RootElement.TryGetProperty("error", out var err) && err.ValueKind != JsonValueKind.Null)
        {
            var msg = err.TryGetProperty("message", out var m) ? m.GetString() : err.ToString();
            throw new InvalidOperationException($"RPC submitblock error: {msg}");
        }

        if (!doc.RootElement.TryGetProperty("result", out var result))
            throw new InvalidOperationException("RPC submitblock response missing required 'result' property");
        if (result.ValueKind == JsonValueKind.Null)
            return null;
        if (result.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException(
                $"RPC submitblock returned unexpected result type: {result.ValueKind}");
        return result.GetString();
    }

    private async Task<T> CallWithRetryAsync<T>(
        HttpClient client,
        string method,
        object? parameters,
        int maxAttempts,
        CancellationToken ct)
    {
        Exception? last = null;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                return await CallInternalAsync<T>(client, method, parameters, ct);
            }
            catch (Exception ex) when (attempt < maxAttempts && IsTransient(ex) && !ct.IsCancellationRequested)
            {
                last = ex;
                var delayMs = 200 * attempt * attempt;
                SoloLog.Warn("RPC transient failure",
                    ("method", method),
                    ("attempt", $"{attempt}/{maxAttempts}"),
                    ("error", FormatException(ex)),
                    ("retry_ms", delayMs));
                await Task.Delay(delayMs, ct);
            }
        }

        throw last ?? new InvalidOperationException($"RPC {method} failed with no exception detail");
    }

    private async Task<T> CallInternalAsync<T>(HttpClient client, string method, object? parameters, CancellationToken ct)
    {
        var payload = BuildPayload(method, parameters);
        using var req = new HttpRequestMessage(HttpMethod.Post, _url);
        req.Headers.Authorization = _auth;
        req.Content = new StringContent(payload, Encoding.UTF8, "application/json");

        using var resp = await SendWithDetailAsync(client, req, method, ct);
        // Stream deserialize: avoids holding a full GBT JSON string + object graph peak (B1).
        // bitcoind often returns HTTP 500 with a JSON-RPC error body — still parse first.
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        RpcEnvelope<T>? envelope = null;
        Exception? parseEx = null;
        try
        {
            envelope = await JsonSerializer.DeserializeAsync<RpcEnvelope<T>>(stream, SerializerOptions, ct);
        }
        catch (Exception ex)
        {
            parseEx = ex;
        }

        if (envelope?.Error != null)
            throw new InvalidOperationException($"RPC {method} error: {envelope.Error.Message} ({envelope.Error.Code})");

        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException(
                $"RPC {method} HTTP {(int)resp.StatusCode}: {resp.ReasonPhrase ?? "error"}" +
                (parseEx != null ? $" (parse: {parseEx.Message})" : ""));

        if (envelope is null)
            throw new InvalidOperationException(
                $"Empty/invalid RPC response for {method}" +
                (parseEx != null ? $": {parseEx.Message}" : ""));
        if (envelope.Result is null && typeof(T) != typeof(JsonElement))
            throw new InvalidOperationException($"RPC {method} returned null result");
        return envelope.Result!;
    }

    private static async Task<HttpResponseMessage> SendWithDetailAsync(
        HttpClient client,
        HttpRequestMessage req,
        string method,
        CancellationToken ct)
    {
        try
        {
            return await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"RPC {method} timed out after {client.Timeout.TotalSeconds:0}s (node busy or GBT template very large)",
                ex);
        }
        catch (HttpRequestException ex)
        {
            throw new HttpRequestException(
                $"RPC {method} transport error: {FormatException(ex)}",
                ex,
                ex.StatusCode);
        }
    }

    private static bool IsTransient(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException!)
        {
            if (e is TimeoutException or IOException or SocketException or HttpRequestException)
                return true;
            if (e is TaskCanceledException)
                return true;
        }
        return false;
    }

    private static string FormatException(Exception ex)
    {
        var parts = new List<string>();
        for (var e = ex; e != null; e = e.InnerException!)
        {
            var msg = e.Message?.Trim();
            if (string.IsNullOrEmpty(msg))
                continue;
            if (parts.Count == 0 || !parts[^1].Contains(msg, StringComparison.Ordinal))
                parts.Add($"{e.GetType().Name}: {msg}");
        }
        return parts.Count == 0 ? ex.GetType().Name : string.Join(" | ", parts);
    }

    private static string BuildPayload(string method, object? parameters)
    {
        var id = Interlocked.Increment(ref _id);
        var obj = new Dictionary<string, object?>
        {
            ["jsonrpc"] = "1.0",
            ["id"] = id.ToString(),
            ["method"] = method,
            ["params"] = parameters ?? Array.Empty<object>()
        };
        return JsonSerializer.Serialize(obj, SerializerOptions);
    }

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public void Dispose()
    {
        _client.Dispose();
        _longpollClient.Dispose();
        _handler.Dispose();
    }

    private sealed class RpcEnvelope<T>
    {
        public T? Result { get; set; }
        public RpcError? Error { get; set; }
    }

    private sealed class RpcError
    {
        public int Code { get; set; }
        public string Message { get; set; } = "";
    }
}

internal sealed class SubmitBlockRpcContent : HttpContent
{
    private const int StreamChunkSize = 128 * 1024;
    private static readonly byte[] Suffix = "\"]}"u8.ToArray();
    private readonly byte[] _prefix;
    private readonly string? _blockHex;
    private readonly ReadOnlyMemory<byte> _blockBytes;
    private readonly int _encodedLength;

    private SubmitBlockRpcContent(
        byte[] prefix,
        string? blockHex,
        ReadOnlyMemory<byte> blockBytes,
        int encodedLength)
    {
        _prefix = prefix;
        _blockHex = blockHex;
        _blockBytes = blockBytes;
        _encodedLength = encodedLength;
        Headers.ContentType = new MediaTypeHeaderValue("application/json")
        {
            CharSet = "utf-8"
        };
    }

    public static SubmitBlockRpcContent Create(long id, string blockHex)
    {
        if (string.IsNullOrEmpty(blockHex))
            throw new ArgumentException("block hex is required", nameof(blockHex));
        if ((blockHex.Length & 1) != 0)
            throw new ArgumentException("block hex must have even length", nameof(blockHex));
        foreach (var c in blockHex)
        {
            if (!((uint)(c - '0') <= 9 ||
                  (uint)(c - 'a') <= 5 ||
                  (uint)(c - 'A') <= 5))
                throw new ArgumentException("block hex contains a non-hex character", nameof(blockHex));
        }

        var idText = id.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var prefix = Encoding.UTF8.GetBytes(
            $"{{\"jsonrpc\":\"1.0\",\"id\":\"{idText}\",\"method\":\"submitblock\",\"params\":[\"");
        return new SubmitBlockRpcContent(prefix, blockHex, default, blockHex.Length);
    }

    public static SubmitBlockRpcContent Create(long id, ReadOnlyMemory<byte> blockBytes)
    {
        if (blockBytes.Length < 81)
            throw new ArgumentException("serialized block is too short", nameof(blockBytes));

        var idText = id.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var prefix = Encoding.UTF8.GetBytes(
            $"{{\"jsonrpc\":\"1.0\",\"id\":\"{idText}\",\"method\":\"submitblock\",\"params\":[\"");
        return new SubmitBlockRpcContent(
            prefix, null, blockBytes, checked(blockBytes.Length * 2));
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        SerializeToStreamAsync(stream, context, CancellationToken.None);

    protected override async Task SerializeToStreamAsync(
        Stream stream,
        TransportContext? context,
        CancellationToken cancellationToken)
    {
        await stream.WriteAsync(_prefix, cancellationToken).ConfigureAwait(false);
        var chunk = ArrayPool<byte>.Shared.Rent(Math.Min(StreamChunkSize, _encodedLength));
        try
        {
            if (_blockHex != null)
            {
                for (var offset = 0; offset < _blockHex.Length; offset += chunk.Length)
                {
                    var count = Math.Min(chunk.Length, _blockHex.Length - offset);
                    CopyAscii(_blockHex.AsSpan(offset, count), chunk);
                    await stream.WriteAsync(chunk.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                var inputChunkSize = Math.Max(1, chunk.Length / 2);
                for (var offset = 0; offset < _blockBytes.Length; offset += inputChunkSize)
                {
                    var count = Math.Min(inputChunkSize, _blockBytes.Length - offset);
                    EncodeHex(_blockBytes.Span.Slice(offset, count), chunk);
                    await stream.WriteAsync(
                        chunk.AsMemory(0, count * 2), cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
        await stream.WriteAsync(Suffix, cancellationToken).ConfigureAwait(false);
    }

    protected override void SerializeToStream(
        Stream stream,
        TransportContext? context,
        CancellationToken cancellationToken)
    {
        stream.Write(_prefix);
        var chunk = ArrayPool<byte>.Shared.Rent(Math.Min(StreamChunkSize, _encodedLength));
        try
        {
            if (_blockHex != null)
            {
                for (var offset = 0; offset < _blockHex.Length; offset += chunk.Length)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var count = Math.Min(chunk.Length, _blockHex.Length - offset);
                    CopyAscii(_blockHex.AsSpan(offset, count), chunk);
                    stream.Write(chunk, 0, count);
                }
            }
            else
            {
                var inputChunkSize = Math.Max(1, chunk.Length / 2);
                for (var offset = 0; offset < _blockBytes.Length; offset += inputChunkSize)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var count = Math.Min(inputChunkSize, _blockBytes.Length - offset);
                    EncodeHex(_blockBytes.Span.Slice(offset, count), chunk);
                    stream.Write(chunk, 0, count * 2);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
        stream.Write(Suffix);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = checked((long)_prefix.Length + _encodedLength + Suffix.Length);
        return true;
    }

    private static void CopyAscii(ReadOnlySpan<char> source, Span<byte> destination)
    {
        for (var i = 0; i < source.Length; i++)
            destination[i] = (byte)source[i];
    }

    private static void EncodeHex(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        ReadOnlySpan<byte> digits = "0123456789abcdef"u8;
        for (var i = 0; i < source.Length; i++)
        {
            var value = source[i];
            destination[i * 2] = digits[value >> 4];
            destination[i * 2 + 1] = digits[value & 0x0f];
        }
    }
}

[JsonConverter(typeof(GbtResponseJsonConverter))]
public sealed class GbtResponse
{
    [JsonPropertyName("version")] public uint Version { get; set; }
    [JsonPropertyName("previousblockhash")] public string PreviousBlockhash { get; set; } = "";
    [JsonPropertyName("coinbasevalue")] public long CoinbaseValue { get; set; }
    [JsonPropertyName("target")] public string Target { get; set; } = "";
    [JsonPropertyName("curtime")] public uint CurTime { get; set; }
    [JsonPropertyName("bits")] public string Bits { get; set; } = "";
    [JsonPropertyName("height")] public uint Height { get; set; }
    [JsonPropertyName("transactions")] public GbtTx[] Transactions { get; set; } = Array.Empty<GbtTx>();
    [JsonPropertyName("coinbaseaux")] public GbtCoinbaseAux? CoinbaseAux { get; set; }
    [JsonPropertyName("default_witness_commitment")] public string? DefaultWitnessCommitment { get; set; }
    [JsonPropertyName("longpollid")] public string? LongPollId { get; set; }
    [JsonPropertyName("mintime")] public uint? Mintime { get; set; }
    [JsonPropertyName("vbrequired")] public uint Vbrequired { get; set; }
    [JsonPropertyName("submitold")] public bool? SubmitOld { get; set; }
    [JsonIgnore] internal GbtPackedTransactions? PackedTransactions { get; set; }
    [JsonIgnore]
    public int TransactionCount =>
        PackedTransactions?.Transactions.Count ?? Transactions?.Length ?? 0;
}

public sealed class GbtTx
{
    [JsonPropertyName("data")]
    [JsonConverter(typeof(HexByteArrayJsonConverter))]
    public byte[] Data { get; set; } = Array.Empty<byte>();
    [JsonPropertyName("txid")] public string? TxId { get; set; }
    [JsonPropertyName("hash")] public string? Hash { get; set; }
}

public sealed class GbtCoinbaseAux
{
    [JsonPropertyName("flags")] public string? Flags { get; set; }
}
