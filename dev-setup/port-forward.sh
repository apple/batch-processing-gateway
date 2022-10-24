#!/bin/bash

kubectl -n local-postgresql port-forward svc/local-postgresql 5432:5432 &
postgresql_pid=$!

function stop() {
  kill ${postgresql_pid}
}

trap stop TERM INT

wait
