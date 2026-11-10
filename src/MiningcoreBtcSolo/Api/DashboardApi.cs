using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Net.Sockets;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.P2p;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;

namespace MiningcoreBtcSolo.Api;

public sealed class DashboardApi
{
    private readonly AppConfig _cfg;
    private readonly MetricsStore _metrics;
    private readonly TemplateEngine _engine;
    private readonly P2pFastPeerManager _p2pFast;
    private readonly string _dashboardHtml;
    private readonly string _p2pFastHtml;
    private readonly string _managementSessionToken = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));

    public DashboardApi(AppConfig cfg, MetricsStore metrics, TemplateEngine engine, P2pFastPeerManager p2pFast)
    {
        _cfg = cfg;
        _metrics = metrics;
        _engine = engine;
        _p2pFast = p2pFast;
        _dashboardHtml = LoadDashboardHtml();
        _p2pFastHtml = LoadAssetHtml("p2p-fast-peers.html");
    }

    public async Task RunAsync(CancellationToken ct)
    {
        if (!_cfg.Api.Enabled || _cfg.Api.ListenPort <= 0)
        {
            SoloLog.Info("API dashboard disabled");
            return;
        }

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://{_cfg.Api.ListenAddr}:{_cfg.Api.ListenPort}");
        builder.Logging.ClearProviders();

        var app = builder.Build();

        if (Guid.TryParse(_cfg.Api.P2pFastPeerUuid, out var adminUuid))
        {
            app.MapGet("/{uuid:guid}", (Guid uuid, HttpContext context) =>
            {
                if (uuid != adminUuid)
                    return Results.NotFound();

                context.Response.Cookies.Append("p2p_fast_admin", _managementSessionToken,
                    new CookieOptions
                    {
                        HttpOnly = true,
                        Secure = context.Request.IsHttps ||
                                 string.Equals(context.Request.Headers["X-Forwarded-Proto"], "https", StringComparison.OrdinalIgnoreCase),
                        SameSite = SameSiteMode.Strict,
                        Path = "/",
                        MaxAge = TimeSpan.FromHours(12)
                    });
                return Results.Redirect("/p2p-fast-peers");
            });

            app.MapGet("/p2p-fast-peers", (HttpContext context) => HasManagementSession(context)
                ? Results.Content(_p2pFastHtml, "text/html; charset=utf-8")
                : Results.NotFound());

            app.MapGet("/api/p2p-fast/peers", async (HttpContext context, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                return Results.Json(await _p2pFast.GetPeersAsync(token));
            });

            app.MapGet("/api/p2p-fast/auto", async (HttpContext context, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                return Results.Json(await _p2pFast.GetAutoStatusAsync(token));
            });

            app.MapPut("/api/p2p-fast/auto", async (
                HttpContext context,
                P2pFastAutoSettings settings,
                CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    return Results.Json(await _p2pFast.UpdateAutoSettingsAsync(settings, token));
                }
                catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
                catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
            });

            app.MapDelete("/api/p2p-fast/auto/blacklist", async (
                HttpContext context,
                CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    return Results.Json(await _p2pFast.ClearBlacklistAsync(token));
                }
                catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
            });

            app.MapPost("/api/p2p-fast/peers", async (HttpContext context, PeerRequest request, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    await _p2pFast.AddPeerAsync(request.Peer, token);
                    return Results.Json(await _p2pFast.GetPeersAsync(token));
                }
                catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
                catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
            });

            app.MapDelete("/api/p2p-fast/peers", async (HttpContext context, string peer, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    if (!await _p2pFast.RemovePeerAsync(peer, token))
                        return Results.NotFound(new { error = "Managed peer not found" });
                    return Results.Json(await _p2pFast.GetPeersAsync(token));
                }
                catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
            });

            app.MapPost("/api/p2p-fast/peers/import", async (HttpContext context, string[] peers, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    await _p2pFast.ImportPeersAsync(peers, token);
                    return Results.Json(await _p2pFast.GetPeersAsync(token));
                }
                catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
                catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
            });

            app.MapGet("/api/p2p-fast/peers/export", async (HttpContext context, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                var json = JsonSerializer.Serialize(await _p2pFast.ExportManagedPeersAsync(token), AppConfig.JsonOptions());
                return Results.File(Encoding.UTF8.GetBytes(json), "application/json", "p2p-fast-peers.json");
            });

            app.MapPost("/api/p2p-fast/peers/probe", async (HttpContext context, string peer, CancellationToken token) =>
            {
                if (!HasManagementSession(context))
                    return Results.Unauthorized();
                try
                {
                    var latency = await _p2pFast.ProbePeerAsync(peer, token);
                    return Results.Json(new { peer, tcp_ping_ms = latency });
                }
                catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
                catch (Exception ex) when (ex is IOException or SocketException or InvalidOperationException)
                {
                    return Results.Json(new { error = ex.Message }, statusCode: 502);
                }
            });
        }
        else
        {
            SoloLog.Warn("p2p fast peer management disabled; api.p2p_fast_peer_uuid must be a UUID");
        }

        app.MapGet("/", () => Results.Content(_dashboardHtml, "text/html; charset=utf-8"));
        app.MapGet("/index.html", () => Results.Content(_dashboardHtml, "text/html; charset=utf-8"));
        app.MapGet("/healthz", () => Results.Text("OK"));
        app.MapGet("/readyz", () =>
        {
            var job = _engine.AuthoritativeJob;
            return job.Ready && _metrics.LastRefreshOk
                ? Results.Text("OK")
                : Results.Text("NOT_READY", statusCode: 503);
        });

        app.MapGet("/api/stats", () =>
        {
            var job = _engine.AuthoritativeJob;
            var best = _metrics.BestShare;
            var hashrateHps = _metrics.EstimateTotalHps();
            var payload = new Dictionary<string, object?>
            {
                ["gateway_version"] = AppInfo.DashboardVersion,
                ["uptime_seconds"] = _metrics.UptimeSeconds,
                // Prefer hashrate_hps (SI). hashrate_th_s kept for older clients.
                ["hashrate_hps"] = hashrateHps,
                ["hashrate_th_s"] = hashrateHps / 1e12,
                ["hashrate_window_secs"] = MetricsStore.HashrateWindowMs / 1000,
                ["connections"] = _metrics.Connections,
                ["subscriptions"] = _metrics.Subscriptions,
                ["current_height"] = job.Ready ? job.Height : 0,
                ["current_value_btc"] = job.Ready ? job.CoinbaseValue / 100_000_000.0 : 0,
                ["txn_count"] = job.Ready ? job.TransactionCount : 0,
                ["network_difficulty"] = job.Ready ? job.NetworkDifficulty : 0,
                ["network_hashrate_hps"] = _metrics.NetworkHashrateHps,
                ["ready"] = job.Ready && _metrics.LastRefreshOk,
                ["last_refresh_ok"] = _metrics.LastRefreshOk,
                ["last_refresh_ms"] = _metrics.LastRefreshMs,
                ["shares_valid"] = _metrics.SharesValid,
                ["shares_error"] = _metrics.SharesError,
                ["shares_late"] = _metrics.SharesLate,
                ["shares_stale"] = _metrics.SharesStale,
                ["shares_unknown_job"] = _metrics.SharesUnknownJob,
                ["share_validation_samples"] = _metrics.ShareValidationSamples,
                ["share_validation_avg_ms"] = _metrics.ShareValidationAverageMs,
                ["share_validation_max_ms"] = _metrics.ShareValidationMaxMs,
                ["share_accepted_ack_queued"] = _metrics.AcceptedShareAckQueued,
                ["share_accepted_ack_written"] = _metrics.AcceptedShareAckWritten,
                ["share_accepted_ack_queue_avg_ms"] = _metrics.AcceptedShareAckQueueAverageMs,
                ["share_accepted_ack_queue_max_ms"] = _metrics.AcceptedShareAckQueueMaxMs,
                ["share_accepted_ack_write_avg_ms"] = _metrics.AcceptedShareAckWriteAverageMs,
                ["share_accepted_ack_write_max_ms"] = _metrics.AcceptedShareAckWriteMaxMs,
                ["clean_broadcasts"] = _metrics.CleanBroadcasts,
                ["clean_broadcast_client_timeouts"] = _metrics.CleanBroadcastClientTimeouts,
                ["blocks_submitted"] = _metrics.BlocksSubmitted,
                ["blocks_accepted"] = _metrics.BlocksAccepted,
                ["best_share_available"] = best != null,
                // Privacy: UA only (no BTC address / IP / session id in label)
                ["best_share_user_agent"] = best?.UserAgent,
                ["best_share_extranonce1"] = best?.Extranonce1,
                ["best_share_difficulty"] = best?.Difficulty ?? 0,
                ["best_share_actual_diff"] = best?.ActualDiff ?? 0,
                ["best_share_timestamp_ms"] = best?.TimestampMs ?? 0
            };
            return Results.Json(payload);
        });

        app.MapGet("/api/workers", () => Results.Json(_metrics.GetWorkers()));
        app.MapGet("/api/shares", () => Results.Json(_metrics.GetShares()));
        app.MapGet("/api/blocks", () => Results.Json(_metrics.GetBlocks()));

        SoloLog.Info("HTTP API dashboard running",
            ("addr", $"{_cfg.Api.ListenAddr}:{_cfg.Api.ListenPort}"));
        await app.RunAsync(ct);
    }

    private static string LoadDashboardHtml()
        => LoadAssetHtml("dashboard.html").Replace("Solo Mining Server", "Miningcore BTC Solo", StringComparison.Ordinal);

    private bool HasManagementSession(HttpContext context)
    {
        if (!context.Request.Cookies.TryGetValue("p2p_fast_admin", out var token))
            return false;
        var actual = Encoding.UTF8.GetBytes(token);
        var expected = Encoding.UTF8.GetBytes(_managementSessionToken);
        return actual.Length == expected.Length && CryptographicOperations.FixedTimeEquals(actual, expected);
    }

    private static string LoadAssetHtml(string assetName)
    {
        var asm = Assembly.GetExecutingAssembly();
        var name = asm.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith(assetName, StringComparison.OrdinalIgnoreCase));
        if (name != null)
        {
            using var s = asm.GetManifestResourceStream(name)!;
            using var r = new StreamReader(s, Encoding.UTF8);
            return r.ReadToEnd();
        }

        var path = Path.Combine(AppContext.BaseDirectory, "Assets", assetName);
        if (File.Exists(path))
            return File.ReadAllText(path);

        return $"<html><body><h1>Miningcore BTC Solo</h1><p>{assetName} asset missing.</p></body></html>";
    }

    private sealed record PeerRequest(string Peer);
}
