#!/bin/bash

set -Eeu

minikube update-context

installed=$(helm list --all-namespaces | grep -v '^NAME' | awk '{ print $1 }')
echo "${installed}"
 
kubectl delete namespace spark-applications || echo "spark-applications Namespace already gone. Ok..."

grep -q local-spark-operator <<<${installed} && \
  helm uninstall -n local-spark-operator local-spark-operator

grep -q local-yunikorn <<<${installed} &&  \
  helm uninstall -n local-yunikorn local-yunikorn

grep -q local-mariadb <<<${installed} && \
  helm uninstall -n local-mariadb local-mariadb

kubectl -n local-ozone delete -f minikube/ozone | echo "Did not remove all ozone objects. Ok..."
kubectl delete namespace local-ozone | echo "local-ozone namespace already gone. Ok..."
kubectl delete namespace local-mariadb | echo "local-mariadb namespace already gone. Ok..."
kubectl delete namespace local-yunikorn | echo "local-yunikorn namespace already gone. Ok..."
kubectl delete namespace local-spark-operator | echo "local-spark-operator namespace already gone. Ok..."

minikube ssh "sudo rm -rf /tmp/hostpath-provisioner/local-mariadb /tmp/hostpath-provisioner/local-ozone"

echo "All components removed."
