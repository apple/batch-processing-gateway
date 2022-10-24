#!/bin/bash
#
# This script install the YuniKorn helm chart
#

installed=$(helm list --all-namespaces | grep -v '^NAME' | awk '{ print $1 }')

grep -q local-yunikorn <<<${installed} || \
  helm install -n local-yunikorn --create-namespace \
    --repo https://apache.github.io/yunikorn-release \
    local-yunikorn yunikorn
