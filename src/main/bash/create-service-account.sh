#!/usr/bin/env bash

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-operator-api-user
EOF


kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-operator-api-user-crb
subjects:
  - kind: ServiceAccount
    name: spark-operator-api-user
    namespace: default
roleRef:
  kind: ClusterRole
  name: spark-operator-api-user-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-operator-api-user-role
rules:
  - apiGroups:
      - ""
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch", "delete"]
  - apiGroups:
      - ""
    resources:
      - events
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - ""
    resources:
      - resourcequotas
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - apiextensions.k8s.io
    resources:
      - customresourcedefinitions
    verbs:
      - get
  - apiGroups:
      - sparkoperator.k8s.io
    resources:
      - sparkapplications
      - sparkapplications/status
      - scheduledsparkapplications
      - scheduledsparkapplications/status
    verbs:
      - create
      - delete
      - get
      - deletecollection
      - list
      - patch
      - update
      - watch
EOF


TOKEN_NAME=$(kubectl get serviceaccount/spark-operator-api-user -o jsonpath='{.secrets[0].name}')

TOKEN=$(kubectl get secret "$TOKEN_NAME" -o jsonpath='{.data.token}' | base64 --decode)

echo "$TOKEN"
