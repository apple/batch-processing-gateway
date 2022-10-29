#!/bin/bash
#
# Create bucket needed
#

aws s3api --endpoint http://localhost:9878 create-bucket --bucket bpg
aws s3api --endpoint http://localhost:9878 put-object --bucket bpg --key eventlog/
aws s3api --endpoint http://localhost:9878 put-object --bucket bpg --key warehouse/
