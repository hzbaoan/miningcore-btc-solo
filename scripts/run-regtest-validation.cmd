@echo off
setlocal
cd /d "%~dp0.."
echo ==^> Building MiningcoreBtcSolo (.NET 10 Release)
dotnet publish src\MiningcoreBtcSolo\MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
if errorlevel 1 exit /b %errorlevel%

echo ==^> Building regtest harness
dotnet build src\MiningcoreBtcSolo.Regtest\MiningcoreBtcSolo.Regtest.csproj -c Release
if errorlevel 1 exit /b %errorlevel%

set MODE=%~1
if "%MODE%"=="" set MODE=all
echo ==^> Running regtest validation mode=%MODE%
if /I "%MODE%"=="suite" (
  for %%M in (vardiff encoding shutdown safety synthetic-gbt all p2p-fast lifecycle core-restart extranonce large-mempool) do (
    echo ==^> Running regtest validation mode=%%M
    dotnet run --project src\MiningcoreBtcSolo.Regtest\MiningcoreBtcSolo.Regtest.csproj -c Release --no-build -- %%M
    if errorlevel 1 exit /b 1
  )
  echo ALL 11 NON-REDUNDANT REGTEST MODES PASSED
  exit /b 0
)
dotnet run --project src\MiningcoreBtcSolo.Regtest\MiningcoreBtcSolo.Regtest.csproj -c Release --no-build -- %*
exit /b %errorlevel%
