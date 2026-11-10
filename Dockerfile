# BTC-only solo Stratum gateway (.NET 10 LTS)
FROM mcr.microsoft.com/dotnet/sdk:10.0-noble AS build
WORKDIR /src

COPY MiningcoreBtcSolo.sln ./
COPY src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj src/MiningcoreBtcSolo/
RUN dotnet restore src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj

COPY src/MiningcoreBtcSolo/ src/MiningcoreBtcSolo/
RUN dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=true

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 solo \
    && useradd --system --uid 10001 --gid solo --home-dir /app --shell /usr/sbin/nologin solo \
    && mkdir -p /app/data/pending-blocks /app/data/failed-blocks \
    && chown -R solo:solo /app/data

COPY --from=build /app/publish ./

ENV SOLO_CONFIG_PATH=/app/config.json \
    DOTNET_EnableDiagnostics=0 \
    ASPNETCORE_URLS=

USER solo
EXPOSE 3333 7152

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:7152/healthz || exit 1

ENTRYPOINT ["./MiningcoreBtcSolo"]
CMD ["--config", "/app/config.json"]
