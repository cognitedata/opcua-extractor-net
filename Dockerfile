FROM mcr.microsoft.com/dotnet/runtime:8.0

VOLUME /config
VOLUME /logs
VOLUME /certificates

COPY /deploy /extractor

WORKDIR /extractor

# Update distro packages
RUN apt-get -qq update \
    && apt-get -qq upgrade \
    && apt-get -qq clean \
    && rm -rf /var/lib/apt/lists/*

COPY /config/config.remote.yml /config_remote/config.yml
COPY /config/opc.ua.net.extractor.Config.xml /config_remote

ENV OPCUA_CONFIG_DIR="/config"
ENV OPCUA_CERTIFICATE_DIR="/certificates"
ENV OPCUA_OWN_CERTIFICATE_DIR="/certificates/pki/own"
ENV OPCUA_CERTIFICATE_SUBJECT="CN=Opcua-extractor, C=NO, S=Oslo, O=Cognite, DC=docker.opcua"

RUN mkdir -p /logs /certificates
RUN chmod -R a+rw /logs
RUN chmod -R a+rw /certificates

RUN groupadd -g 1000 extractor && useradd -m -u 1000 -g extractor extractor
USER extractor

ENTRYPOINT ["dotnet", "/extractor/OpcuaExtractor.dll"]
