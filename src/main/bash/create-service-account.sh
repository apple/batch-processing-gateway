#!/usr/bin/env bash

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-operator-api-user
EOF

TOKEN_NAME=$(kubectl get serviceaccount/spark-operator-api-user -o jsonpath='{.secrets[0].name}')

TOKEN=$(kubectl get secret "$TOKEN_NAME" -o jsonpath='{.data.token}' | base64 --decode)

echo "$TOKEN"
