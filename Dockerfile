FROM mcr.microsoft.com/dotnet/runtime:6.0
VOLUME /config
VOLUME /logs
VOLUME /certificates
COPY /deploy /extractor
WORKDIR /extractor

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_CERTIFICATE_DIR="/certificates"
ENV OPCUA_OWN_CERTIFICATE_DIR="/certificates/pki/own"
ENV OPCUA_CERTIFICATE_SUBJECT="CN=Opcua-extractor, C=NO, S=Oslo, O=Cognite, DC=docker.opcua"

ENTRYPOINT ["dotnet", "/extractor/OpcuaExtractor.dll"]
