namespace MiningcoreBtcSolo.Stratum;

internal readonly record struct VarDiffDecision(
    bool ResetWindow,
    bool ApplyDifficulty,
    double NextDifficulty,
    bool BurstUp,
    double SmoothedRatio);

/// <summary>
/// Pure VarDiff decision logic. AccumulatedWork is measured in difficulty units,
/// which lets shares from older assigned work contribute without multiplying the new
/// difficulty a second time.
/// </summary>
internal static class VarDiffCalculator
{
    public static VarDiffDecision Evaluate(
        DifficultyConfig config,
        double currentDifficulty,
        int shareCount,
        double accumulatedWork,
        double elapsedSeconds,
        bool allowBurst,
        double previousSmoothedRatio = 1.0)
    {
        if (currentDifficulty <= 0)
            return default;

        var elapsed = Math.Max(0.05, elapsedSeconds);
        var target = Math.Max(0.5, config.TargetTimeSecs);
        var window = Math.Max(1.0, config.RetargetTimeSecs);
        var burstShares = Math.Max(2, config.RetargetShareBurst);
        var windowElapsed = elapsed >= window;
        var burst = allowBurst && !windowElapsed && shareCount >= burstShares;

        if (!burst && !windowElapsed)
            return default;

        var maxDown = Math.Clamp(config.MaxStepDown, 0.05, 0.95);
        double next;

        if (shareCount <= 0 || accumulatedWork <= 0)
        {
            // Silence is evidence that the current target is too hard. Move down
            // one configured step per complete window instead of waiting for a share.
            next = currentDifficulty * maxDown;
        }
        else
        {
            // Each accepted share represents work equal to the difficulty target it
            // satisfied. This remains valid when older work used its assigned target.
            var idealDifficulty = accumulatedWork * target / elapsed;
            var ratio = idealDifficulty / currentDifficulty;
            var variance = Math.Clamp(config.VariancePercent / 100.0, 0, 0.9);
            var lowerRatio = 1.0 / (1.0 + variance);
            var upperRatio = 1.0 / Math.Max(0.1, 1.0 - variance);

            if (burst)
            {
                // Burst is an early upward-only path. If the weighted samples do not
                // exceed the stable band, keep collecting until the normal window closes.
                if (ratio <= upperRatio)
                    return default;

                var maxUpBurst = Math.Max(config.MaxStepUp, config.MaxStepUpBurst);
                next = currentDifficulty * Math.Min(ratio, maxUpBurst);
            }
            else
            {
                // A normal 30s/5s window expects only six shares, whose natural Poisson
                // deviation is about 41%. Smooth across windows before changing stable miners.
                var smoothing = Math.Clamp(config.RetargetSmoothing, 0.05, 1.0);
                var previous = double.IsFinite(previousSmoothedRatio) && previousSmoothedRatio > 0
                    ? previousSmoothedRatio
                    : 1.0;
                var smoothedRatio = previous + smoothing * (ratio - previous);

                // variance_percent is expressed as accepted-share interval variance.
                // Difficulty ratio is the inverse, so convert the band before comparing.
                if (smoothedRatio >= lowerRatio && smoothedRatio <= upperRatio)
                    return new VarDiffDecision(true, false, currentDifficulty, false, smoothedRatio);

                var maxUp = Math.Max(1.1, config.MaxStepUp);
                next = currentDifficulty * Math.Clamp(smoothedRatio, maxDown, maxUp);
            }
        }

        next = Math.Clamp(next, config.Min, config.Max);
        var relativeChange = Math.Abs(next - currentDifficulty) / Math.Max(currentDifficulty, 1e-12);

        if (relativeChange < 1e-9)
            return new VarDiffDecision(true, false, currentDifficulty, false, 1.0);

        return new VarDiffDecision(true, true, next, burst && next > currentDifficulty, 1.0);
    }
}
