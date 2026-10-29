# Pre-mainnet regtest validation orchestrator
# Builds with .NET 10, runs share -> submitblock -> on-chain checks.
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

Write-Host "==> Building MiningcoreBtcSolo (.NET 10 Release)"
dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> Building regtest harness"
dotnet build src/MiningcoreBtcSolo.Regtest/MiningcoreBtcSolo.Regtest.csproj -c Release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$mode = if ($args.Count -gt 0) { $args[0] } else { "all" }
$extraArgs = if ($args.Count -gt 1) { $args[1..($args.Count - 1)] } else { @() }

if ($mode -eq "suite") {
    $suiteModes = @(
        "vardiff", "encoding", "shutdown", "safety", "synthetic-gbt",
        "all", "p2p-fast", "lifecycle", "core-restart", "extranonce", "large-mempool"
    )
    foreach ($suiteMode in $suiteModes) {
        Write-Host "==> Running regtest validation mode=$suiteMode"
        dotnet run --project src/MiningcoreBtcSolo.Regtest/MiningcoreBtcSolo.Regtest.csproj -c Release --no-build -- $suiteMode @extraArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    Write-Host "ALL 11 NON-REDUNDANT REGTEST MODES PASSED"
    exit 0
}

Write-Host "==> Running regtest validation mode=$mode"
dotnet run --project src/MiningcoreBtcSolo.Regtest/MiningcoreBtcSolo.Regtest.csproj -c Release --no-build -- $mode @extraArgs
exit $LASTEXITCODE
