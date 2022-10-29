#!/bin/bash
#
# This script create the namespace local-ozone and deploy Ozone to mock S3
#

installed=$(helm list --all-namespaces | grep -v '^NAME' | awk '{ print $1 }')

kubectl create namespace local-ozone || echo "local-ozone Namespace exists. Ok..."

IMAGE_REGISTRY="apache"
IMAGE_REPO="ozone"
IMAGE_TAG="1.2.1"

if [ "$(uname -s)" = "Darwin" ] && [ "$(arch)" = "arm64" ]; then
  # Replace Ozone docker image for MacOSX running on ARM64 architecture
  IMAGE_REGISTRY="leletan"
  IMAGE_TAG="1.2.1-20220623-1-aarch64"
fi

grep -q local-ozone <<<${installed} || \
  helm install -n local-ozone --create-namespace \
    --set image.registry="${IMAGE_REGISTRY}" \
    --set image.repository="${IMAGE_REPO}" \
    --set image.tag="${IMAGE_TAG}" \
    local-ozone ./ozone





