using System.Net.Sockets;
using MiningcoreBtcSolo;
using MiningcoreBtcSolo.Api;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.P2p;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Stratum;
using MiningcoreBtcSolo.Submit;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

// miningcore-btc-solo — BTC-only true solo gateway
// Template path: GBT longpoll + optional ZMQ + optional P2P empty clean jobs
// Coinbase: fixed address from config (no pool payouts, no SV2)

var configPath = ResolveConfigPath(args);
SoloLog.Info("loading config", ("path", configPath));
var cfg = AppConfig.Load(configPath);
SoloLog.Configure(cfg.LogLevel);

var metrics = new MetricsStore();
var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, cfg.Bitcoind.RpcUser, cfg.Bitcoind.RpcPassword);
var submitQueue = new BlockSubmitQueue(cfg, rpc, metrics);
var engine = new TemplateEngine(cfg, rpc, metrics, submitQueue);
var stratum = new StratumServer(cfg, engine, metrics);
var p2pFast = new P2pFastPeerManager(cfg, engine, rpc);
var api = new DashboardApi(cfg, metrics, engine, p2pFast);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

SoloLog.Info("booting miningcore-btc-solo",
    ("network", cfg.NetworkName),
    ("stratum", $"{cfg.Stratum.ListenAddr}:{cfg.Stratum.ListenPort}"),
    ("api", $"{cfg.Api.ListenAddr}:{cfg.Api.ListenPort}"),
    ("coinbase", cfg.Coinbase.Address),
    ("rpc", cfg.Bitcoind.RpcUrl),
    ("data_dir", cfg.Runtime.DataDir));

// Fail closed: wrong chain would burn hashrate on a non-mainnet tip.
await ChainGuard.VerifyAsync(cfg, rpc, cts.Token);

// Recover any fsynced blocks from a previous crash, then start the submit consumer.
await submitQueue.StartAsync(cts.Token);

await engine.StartAsync(cts.Token);

var tasks = new List<Task> { stratum.RunAsync(cts.Token) };

if (p2pFast.IsEnabled)
    tasks.Add(p2pFast.RunAsync(cts.Token));

if (cfg.Api.Enabled && cfg.Api.ListenPort > 0)
    tasks.Add(api.RunAsync(cts.Token));

try
{
    await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(tasks, cts.Token);
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    // shutdown
}
finally
{
    // Stop network-facing tasks first so no client can enqueue a new block while
    // the submit queue is draining. Stratum also awaits all active client handlers.
    cts.Cancel();
    try
    {
        await Task.WhenAll(tasks);
    }
    catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException)
    {
        SoloLog.Debug("network task stopped during shutdown", ("error", ex.Message));
    }
    catch (Exception ex)
    {
        SoloLog.Error("network task failed during shutdown", ("error", ex.Message));
    }

    try
    {
        await submitQueue.StopAsync(TimeSpan.FromMinutes(2));
    }
    catch (Exception ex)
    {
        SoloLog.Alert("submit queue graceful shutdown failed", ("error", ex.Message));
    }
}

SoloLog.Info("shutdown");

static string ResolveConfigPath(string[] args)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (args[i] is "--config" or "-c")
            return args[i + 1];
    }

    var env = Environment.GetEnvironmentVariable("SOLO_CONFIG_PATH");
    if (!string.IsNullOrWhiteSpace(env))
        return env;

    foreach (var candidate in new[]
             {
                 "config.json",
                 Path.Combine(AppContext.BaseDirectory, "config.json"),
                 "/app/config.json"
             })
    {
        if (File.Exists(candidate))
            return candidate;
    }

    throw new FileNotFoundException("config.json not found. Pass --config PATH or set SOLO_CONFIG_PATH.");
}

internal static class ServiceTaskSupervisor
{
    public static async Task WaitForShutdownOrFailureAsync(
        IReadOnlyCollection<Task> tasks,
        CancellationToken shutdownToken)
    {
        if (tasks.Count == 0)
            throw new InvalidOperationException("No network services were started");

        var completed = await Task.WhenAny(tasks);
        if (shutdownToken.IsCancellationRequested)
            return;

        // Await the completed task itself so SocketException/IOException and all
        // other service failures reach the process entry point and produce a
        // non-zero exit code. A clean early return is also a fatal service stop.
        await completed;
        throw new InvalidOperationException("A network service stopped unexpectedly");
    }
}
