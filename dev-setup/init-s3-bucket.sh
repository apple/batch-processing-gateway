#!/bin/bash
#
# Create bucket needed
#

export AWS_ACCESS_KEY_ID="${USER}"
export AWS_SECRET_ACCESS_KEY=$(openssl rand -hex 32)

kubectl -n local-ozone port-forward svc/s3g 9878:9878 &
fwd_pid=$!
aws s3api --endpoint http://localhost:9878 create-bucket --bucket bpg
aws s3api --endpoint http://localhost:9878 put-object --bucket bpg --key eventlog/
aws s3api --endpoint http://localhost:9878 put-object --bucket bpg --key warehouse/
kill ${fwd_pid} || echo "No forward pid to kill"
