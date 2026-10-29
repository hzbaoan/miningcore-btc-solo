using System.Reflection;
using System.Text;
using MiningcoreBtcSolo.Metrics;
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
    private readonly string _dashboardHtml;

    public DashboardApi(AppConfig cfg, MetricsStore metrics, TemplateEngine engine)
    {
        _cfg = cfg;
        _metrics = metrics;
        _engine = engine;
        _dashboardHtml = LoadDashboardHtml();
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
    {
        var asm = Assembly.GetExecutingAssembly();
        var name = asm.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith("dashboard.html", StringComparison.OrdinalIgnoreCase));
        if (name != null)
        {
            using var s = asm.GetManifestResourceStream(name)!;
            using var r = new StreamReader(s, Encoding.UTF8);
            return r.ReadToEnd().Replace("Solo Mining Server", "Miningcore BTC Solo", StringComparison.Ordinal);
        }

        var path = Path.Combine(AppContext.BaseDirectory, "Assets", "dashboard.html");
        if (File.Exists(path))
            return File.ReadAllText(path).Replace("Solo Mining Server", "Miningcore BTC Solo", StringComparison.Ordinal);

        return "<html><body><h1>Miningcore BTC Solo</h1><p>Dashboard asset missing.</p></body></html>";
    }
}
