FROM mcr.microsoft.com/dotnet/sdk:5.0
VOLUME /config
VOLUME /logs
COPY /deploy /bridge
WORKDIR /bridge

ENV OPCUA_CONFIG_DIR="/config"

ENTRYPOINT ["dotnet", "/bridge/MQTTCDFBridge.dll"]
