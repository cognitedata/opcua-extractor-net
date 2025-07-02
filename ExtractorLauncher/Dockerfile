FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

USER app
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG configuration=Release
WORKDIR /src
COPY ["ExtractorLauncher/ExtractorLauncher.csproj", "ExtractorLauncher/"]
RUN dotnet restore "ExtractorLauncher/ExtractorLauncher.csproj"
COPY . .
WORKDIR "/src/ExtractorLauncher"
RUN dotnet build "ExtractorLauncher.csproj" -c $configuration -o /app/build

FROM build AS publish
ARG configuration=Release
RUN dotnet publish "ExtractorLauncher.csproj" -c $configuration -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OpcuaExtractor.dll"]
