FROM eu.gcr.io/cognitedata/dotnet-mono:2.2-sdk
VOLUME /config
COPY /deploy /extractor
WORKDIR /extractor

ENTRYPOINT ["dotnet", "/extractor/Extractor.dll", "/config"]
