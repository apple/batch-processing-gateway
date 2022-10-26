#!/bin/bash
#
# This script create the namespace local-ozone and deploy Ozone to mock S3
#

# Create namespace to deploy Ozone
kubectl create namespace local-ozone | echo "local-ozone namespace already exists. Ok..."

# Replace Ozone docker image for MacOSX running on ARM64 architecture
if [ "$(uname -s)" = "Darwin" ] && [ "$(arch)" = "arm64" ]; then
  X86_IMAGE="apache/ozone:1.2.1"
  AMD64_IMAGE="leletan/ozone:1.2.1-20220623-1-aarch64"
  rm ozone/*.bak
  find ozone/*-statefulset.yaml -type f -exec sed -i '.bak' "s~${X86_IMAGE}~${AMD64_IMAGE}~g" {} \;
  rm ozone/*.bak
fi

# Deploy Ozone resources
kubectl -n local-ozone apply -f ozone
