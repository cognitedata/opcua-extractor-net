FROM eu.gcr.io/cognitedata/dotnet-mono:2.2-sdk
VOLUME /config
VOLUME /logs
COPY /deploy /extractor
WORKDIR /extractor

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_LOGGER_DIR="/logs"

ENTRYPOINT ["dotnet", "/extractor/Extractor.dll"]
