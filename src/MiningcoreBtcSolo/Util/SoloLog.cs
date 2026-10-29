using MiningcoreBtcSolo.Template;

namespace MiningcoreBtcSolo.Util;

/// <summary>
/// Structured console logging:
/// 2026-07-23T18:43:02.482431Z  INFO template update source=longpoll height=...
/// Level is filtered by <see cref="Configure"/> from config log_level (default Information).
/// </summary>
public static class SoloLog
{
    private static readonly object Gate = new();
    private static SoloLogLevel _minLevel = SoloLogLevel.Information;

    public static void Configure(string? level)
    {
        _minLevel = ParseLevel(level);
    }

    public static SoloLogLevel MinLevel => _minLevel;

    public static void Debug(string message)
    {
        if (_minLevel <= SoloLogLevel.Debug)
            Write("DEBUG", message, Console.Out);
    }

    public static void Info(string message)
    {
        if (_minLevel <= SoloLogLevel.Information)
            Write("INFO", message, Console.Out);
    }

    public static void Warn(string message)
    {
        if (_minLevel <= SoloLogLevel.Warning)
            Write("WARN", message, Console.Out);
    }

    public static void Error(string message)
    {
        if (_minLevel <= SoloLogLevel.Error)
            Write("ERROR", message, Console.Error);
    }

    /// <summary>Greppable production alert (stderr ERROR + ALERT prefix). Always emitted.</summary>
    public static void Alert(string message) => Write("ERROR", "ALERT " + message, Console.Error);

    public static void Debug(string message, params (string Key, object? Value)[] fields)
    {
        if (_minLevel <= SoloLogLevel.Debug)
            Debug(FormatMessage(message, fields));
    }

    public static void Info(string message, params (string Key, object? Value)[] fields)
    {
        if (_minLevel <= SoloLogLevel.Information)
            Info(FormatMessage(message, fields));
    }

    public static void Warn(string message, params (string Key, object? Value)[] fields)
    {
        if (_minLevel <= SoloLogLevel.Warning)
            Warn(FormatMessage(message, fields));
    }

    public static void Error(string message, params (string Key, object? Value)[] fields)
    {
        if (_minLevel <= SoloLogLevel.Error)
            Error(FormatMessage(message, fields));
    }

    public static void Alert(string message, params (string Key, object? Value)[] fields)
        => Alert(FormatMessage(message, fields));

    public static string SourceName(TemplateSource source) => source switch
    {
        TemplateSource.Startup => "startup",
        TemplateSource.Longpoll => "longpoll",
        TemplateSource.LongpollFallback => "longpoll_fallback",
        TemplateSource.ZmqHashblock => "zmq_hashblock",
        TemplateSource.ZmqRawblock => "zmq_rawblock",
        TemplateSource.P2pFast => "p2p_fast",
        TemplateSource.PostSubmit => "post_submit",
        _ => source.ToString().ToLowerInvariant()
    };

    public static string FormatTemplateEvent(
        string eventName,
        TemplateSource source,
        string jobId,
        uint height,
        int txs,
        long rewardSat,
        uint nbits,
        bool cleanJobs,
        string? peer = null,
        string? hash = null)
    {
        var fields = new List<(string Key, object? Value)>
        {
            ("source", SourceName(source)),
            ("job_id", jobId),
            ("height", height),
            ("txs", txs),
            ("reward", $"{rewardSat} sat"),
            ("nbits", nbits.ToString("x8")),
            ("clean_jobs", cleanJobs ? "true" : "false")
        };

        if (!string.IsNullOrWhiteSpace(peer))
            fields.Add(("peer", peer));
        if (!string.IsNullOrWhiteSpace(hash))
            fields.Add(("hash", hash));

        return FormatMessage(eventName, fields.ToArray());
    }

    public static string FormatMessage(string message, params (string Key, object? Value)[] fields)
    {
        if (fields.Length == 0)
            return message;
        var parts = new string[fields.Length];
        for (var i = 0; i < fields.Length; i++)
        {
            var v = fields[i].Value;
            var text = v switch
            {
                null => "",
                bool b => b ? "true" : "false",
                IFormattable f when v is not string => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture) ?? "",
                _ => v.ToString() ?? ""
            };
            // Quote values that contain spaces (except already structured reward "N sat").
            if (text.Contains(' ') && !text.EndsWith(" sat", StringComparison.Ordinal))
                text = "\"" + text.Replace("\"", "'", StringComparison.Ordinal) + "\"";
            parts[i] = $"{fields[i].Key}={text}";
        }
        return message + " " + string.Join(' ', parts);
    }

    private static SoloLogLevel ParseLevel(string? level)
    {
        if (string.IsNullOrWhiteSpace(level))
            return SoloLogLevel.Information;
        return level.Trim().ToLowerInvariant() switch
        {
            "debug" or "verbose" or "trace" => SoloLogLevel.Debug,
            "info" or "information" => SoloLogLevel.Information,
            "warn" or "warning" => SoloLogLevel.Warning,
            "error" or "err" => SoloLogLevel.Error,
            "none" or "off" => SoloLogLevel.None,
            _ => SoloLogLevel.Information
        };
    }

    private static void Write(string level, string message, TextWriter writer)
    {
        var ts = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ");
        var line = $"{ts}  {level} {message}";
        lock (Gate)
        {
            writer.WriteLine(line);
            writer.Flush();
        }
    }
}

public enum SoloLogLevel
{
    Debug = 0,
    Information = 1,
    Warning = 2,
    Error = 3,
    /// <summary>Suppress Info/Warn/Error/Debug; Alert still prints.</summary>
    None = 4
}
