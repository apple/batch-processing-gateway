#!/usr/bin/env bash

re="skate-[0-9]+.[0-9]+.[0-9]+"

if [[ "$1" =~ $re ]]; then
   SKATE_VERSION=$1
else
   echo "A skate tag must be provided."
   exit 1
fi

HELM_CHART=${2:-apple-internal/helm/spinnaker-skate}
HELM_REPO=${3:-apple-aiml-skate-spinnaker}
HELM_OUT=".out"
HELM_VERSION="3.8.1"

mkdir -p "${HELM_OUT}/${HELM_REPO}"
wget -qO- https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz | tar xvz
mv linux-amd64/helm /usr/bin/helm

sed -E -i -e "s/(tag:[[:space:]])'skate-[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+'/\1${SKATE_VERSION}/g" ${HELM_CHART}/values.yaml
helm package --destination=${HELM_OUT}/${HELM_REPO} --version=${SKATE_VERSION##*-} ${HELM_CHART}

ci stage-lib "${HELM_OUT}/**"
