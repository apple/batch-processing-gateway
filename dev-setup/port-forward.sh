#!/bin/bash

export AWS_ACCESS_KEY_ID="${USER}"
AWS_SECRET_ACCESS_KEY=$(openssl rand -hex 32)
export AWS_SECRET_ACCESS_KEY
kubectl -n local-ozone port-forward svc/s3g 9878:9878 &
ozone_pid=$!

kubectl -n local-postgresql port-forward svc/local-postgresql 5432:5432 &
postgresql_pid=$!

function stop() {
  kill ${ozone_pid}
  kill ${postgresql_pid}
}

trap stop TERM INT

wait
