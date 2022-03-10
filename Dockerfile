FROM mcr.microsoft.com/dotnet/sdk:6.0
VOLUME /config
VOLUME /logs
VOLUME /certificates
COPY /deploy /extractor
WORKDIR /extractor

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_CERTIFICATE_DIR="/certificates"

ENTRYPOINT ["dotnet", "/extractor/OpcuaExtractor.dll"]
