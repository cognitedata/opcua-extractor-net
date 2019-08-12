#!/bin/sh
set -e
ORG=$1
shift
REPO=$1
shift
TAG=$1
shift
OKPATH=$1
shift

# Build release from tag if it does not exist
data=$(sh $OKPATH list_releases $ORG $REPO \
    | awk -v "tag=$TAG" -F'\t' '$2 == tag { print $3 }')

if [ "$data" = "" ]; then
    echo "No data found, creating release"
    tagdata=$(sh $OKPATH create_release $ORG $REPO $TAG name=$TAG)
fi

uploadpath=$(sh $OKPATH list_releases cognitedata opcua-extractor-net \
    | awk -v "tag=$TAG" -F'\t' '$2 == tag { print $3 }' \
    | xargs -I@ $OKPATH release cognitedata opcua-extractor-net @ _filter='.upload_url')

while [ "$1" != "" ]; do
    name=$(basename $1)
    echo "Uploading $name..."
    echo "$uploadpath" | sed 's/{.*$/?name='"$name"'/' \
        | xargs -I@ $OKPATH upload_asset @ "$1"
    shift
done
