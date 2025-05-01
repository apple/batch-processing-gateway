#!/bin/bash
#
# This script installs the YuniKorn helm chart
#
database=bpg
username=bpg
password=samplepass

installed=$(helm list --all-namespaces | grep -v '^NAME' | awk '{ print $1 }')

grep -q local-postgresql <<<${installed} || \
  helm install -n local-postgresql --create-namespace \
    --set primary.service.type=NodePort \
    --set auth.database=${database} \
    --set auth.username=${username} \
    --set auth.password=${password} \
    local-postgresql oci://registry-1.docker.io/bitnamicharts/postgresql

