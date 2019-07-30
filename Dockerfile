FROM eu.gcr.io/cognitedata/dotnet-mono:2.2-sdk

VOLUME /config
WORKDIR /build
COPY . .

RUN apt-get update && apt-get install -y libxml2-utils

RUN ./credentials.sh
RUN mono .paket/paket.exe install
RUN dotnet build

RUN dotnet publish -c Release -o /build/deploy Extractor
ENTRYPOINT ["dotnet", "/build/deploy/Extractor.dll"]
