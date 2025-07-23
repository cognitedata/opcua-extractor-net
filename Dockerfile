# 1. Build Stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /source

# Copy csproj and restore as distinct layers
COPY ./*.sln .

# The solution file contains Windows-specific projects (Wix, Windows Service) that cannot be built on Linux.
# We remove them from the solution file inside the container to allow `dotnet restore` to succeed.
RUN sed -i '/OpcUaExtractorSetup/d' opcua-extractor-net.sln
RUN sed -i '/OpcUaServiceManager/d' opcua-extractor-net.sln

# Copy all cross-platform project files referenced in the modified solution.
COPY Extractor/*.csproj ./Extractor/
COPY ExtractorLauncher/*.csproj ./ExtractorLauncher/
COPY ConfigurationTool/*.csproj ./ConfigurationTool/
COPY Test/*.csproj ./Test/
COPY MQTTCDFBridge/*.csproj ./MQTTCDFBridge/
COPY Server/*.csproj ./Server/
RUN dotnet restore "opcua-extractor-net.sln"

# Copy everything else and build
COPY . .
WORKDIR /source/ExtractorLauncher
RUN dotnet publish -c Release -o /app/publish --no-restore

# 2. Final Stage
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /extractor

VOLUME /config
VOLUME /logs
VOLUME /certificates

COPY --from=build /app/publish .

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_CERTIFICATE_DIR="/certificates"
ENV OPCUA_OWN_CERTIFICATE_DIR="/certificates/pki/own"
ENV OPCUA_CERTIFICATE_SUBJECT="CN=Opcua-extractor, C=NO, S=Oslo, O=Cognite, DC=docker.opcua"

RUN groupadd -g 1000 extractor && useradd -m -u 1000 -g extractor extractor
USER extractor

ENTRYPOINT ["dotnet", "OpcuaExtractor.dll"]
