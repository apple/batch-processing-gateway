#!/bin/bash
#
# This script create the namespace local-ozone and deploy Ozone to mock S3
#

# Create namespace to deploy Ozone
kubectl create namespace local-ozone | echo "local-ozone namespace already exists. Ok..."
# Deploy Ozone resources
kubectl -n local-ozone apply -f ozone
