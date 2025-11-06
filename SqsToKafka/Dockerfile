# ---------- build stage ----------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# copy csproj & restore first (better layer caching)
COPY *.csproj ./
RUN dotnet restore

# copy the rest and publish (self-contained=false)
COPY . ./
RUN dotnet publish -c Release -o /app/publish

# ---------- runtime stage ----------
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app

# (optional) run as non-root
# RUN useradd -m worker && chown -R worker /app
# USER worker

COPY --from=build /app/publish ./

# default: read config from appsettings.json, env vars, or command-line
ENTRYPOINT ["dotnet", "SqsToKafka.dll"]
