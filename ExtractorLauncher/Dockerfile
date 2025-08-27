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

# docker-entrypoint.sh 스크립트 복사 및 실행 권한 부여
COPY --chown=app:app ExtractorLauncher/docker-entrypoint.sh .
RUN chmod +x ./docker-entrypoint.sh

# 스크립트를 새로운 ENTRYPOINT로 지정
ENTRYPOINT ["./docker-entrypoint.sh"]
# 기존 ENTRYPOINT는 CMD로 변경하여 스크립트에 인자로 전달
CMD ["dotnet", "OpcuaExtractor.dll"]
