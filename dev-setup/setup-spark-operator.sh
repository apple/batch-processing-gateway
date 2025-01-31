#!/bin/bash
#
# This script creates a namespace spark-applications
# and install the Spark operator that checks the spark-applications namespace
#

installed=$(helm list --all-namespaces | grep -v '^NAME' | awk '{ print $1 }')

kubectl create namespace spark-applications || echo "spark-applications Namespace exists. Ok..."

grep -q local-spark-operator <<<${installed} || \
  helm install -n local-spark-operator --create-namespace \
    --repo https://kubeflow.github.io/spark-operator \
    --set sparkJobNamespace=spark-applications \
    local-spark-operator spark-operator
