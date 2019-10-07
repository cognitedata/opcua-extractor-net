FROM eu.gcr.io/cognitedata/dotnet-mono:3.0-sdk
VOLUME /config
VOLUME /logs
VOLUME /certificates
COPY /deploy /extractor
WORKDIR /extractor

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_LOGGER_DIR="/logs"
ENV OPCUA_CERTIFICATE_DIR="/certificates"

ENTRYPOINT ["dotnet", "/extractor/Extractor.dll"]
