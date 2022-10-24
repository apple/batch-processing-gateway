#!/bin/bash
#
# This script generates a configuration YAML for local development.
# Here we assume the PostreSQL and Ozone S3 have already been forwarded to local ports 5432 and 9878.
#

ozoneEndpoint=$(kubectl get service s3g-public --namespace local-ozone -o jsonpath='{.spec.clusterIP}'):9878

dbHostPort=localhost:5432

pysparkImage=apache/spark-py:v3.2.2
sparkImage=apache/spark:v3.2.2

context=$(kubectl config current-context)
serverUrl=$(kubectl config view -o jsonpath='{.clusters[?(@.name == "'${context}'")].cluster.server}')
saSecret=$(kubectl -n spark-applications get sa/local-spark-operator-spark -o json | jq -r '.secrets[] | .name')
saToken=$(kubectl -n spark-applications get secret/${saSecret} -o json | jq -r '.data.token')
saCA=$(kubectl -n spark-applications get secret/${saSecret} -o json | jq -r '.data."ca.crt"')

export AWS_ACCESS_KEY_ID="${USER}"
export AWS_SECRET_ACCESS_KEY=$(openssl rand -hex 32)

cat > bpg-config.yaml <<EOF
defaultSparkConf:
  spark.kubernetes.submission.connectionTimeout: 30000
  spark.kubernetes.submission.requestTimeout: 30000
  spark.kubernetes.driver.connectionTimeout: 30000
  spark.kubernetes.driver.requestTimeout: 30000
sparkClusters:
  - weight: 100
    id: minikube
    eksCluster: ${context}
    masterUrl: ${serverUrl}
    caCertDataSOPS: ${saCA}
    userTokenSOPS: ${saToken}
    userName: spark-operator
    sparkApplicationNamespace: spark-applications
    sparkServiceAccount: local-spark-operator-spark
    sparkVersions:
      - "3.2"
    queues:
      - poc
    ttlSeconds: 259200
    sparkUIUrl: http://localhost:8080
    batchScheduler: yunikorn
    sparkConf:
      spark.kubernetes.executor.podNamePrefix: '{spark-application-resource-name}'
      spark.eventLog.enabled: "true"
      spark.eventLog.dir: s3a://bpg/eventlog
      spark.history.fs.logDirectory: s3a://bpg/eventlog
      spark.sql.warehouse.dir: s3a://bpg/warehouse
      spark.sql.catalogImplementation: hive
      spark.jars.ivy: /opt/spark/work-dir/.ivy2
      spark.hadoop.fs.s3a.connection.ssl.enabled: false
      spark.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY_ID}
      spark.hadoop.fs.s3a.secret.key: ${AWS_SECRET_ACCESS_KEY}
      spark.hadoop.fs.s3a.endpoint: ${ozoneEndpoint}
      spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
      spark.hadoop.fs.s3a.change.detection.version.required: false
      spark.hadoop.fs.s3a.change.detection.mode: none
      spark.hadoop.fs.s3a.fast.upload: true
      spark.jars.packages: org.apache.hadoop:hadoop-aws:3.2.2
      spark.hadoop.fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    sparkUIOptions:
      ServicePort: 4040
      ingressAnnotations:
        nginx.ingress.kubernetes.io/rewrite-target: /\$2
        nginx.ingress.kubernetes.io/proxy-redirect-from: http://\$host/
        nginx.ingress.kubernetes.io/proxy-redirect-to: /spark-applications-4/{spark-application-resource-name}/
        kubernetes.io/ingress.class: nginx
        nginx.ingress.kubernetes.io/configuration-snippet: |
          proxy_set_header Accept-Encoding ""; # disable compression
          sub_filter_last_modified off;
          sub_filter '<head>' '<head> <base href="/spark-applications-4/{spark-application-resource-name}/">'; # add base url
          sub_filter 'href="/' 'href="'; # remove absolute URL path so base url applies
          sub_filter 'src="/' 'src="'; # remove absolute URL path so base url applies

          sub_filter '/{{num}}/jobs/' '/jobs/';

          sub_filter "setUIRoot('')" "setUIRoot('/spark-applications-4/{spark-application-resource-name}/')"; # Set UI root for JS scripts
          sub_filter "document.baseURI.split" "document.documentURI.split"; # Executors page issue fix
          sub_filter_once off;
      ingressTLS:
        - hosts:
            - localhost
          secretName: localhost-tls-secret
    driver:
      env:
        - name: STATSD_SERVER_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        - name: STATSD_SERVER_PORT
          value: "8125"
    executor:
      env:
        - name: STATSD_SERVER_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        - name: STATSD_SERVER_PORT
          value: "8125"
sparkImages:
  - name: ${pysparkImage}
    types:
      - Python
    version: "3.2"
  - name: ${sparkImage}
    types:
      - Java
      - Scala
    version: "3.2"
s3Bucket: bpg
s3Folder: uploaded
sparkLogS3Bucket: bpg
sparkLogIndex: index/index.txt
batchFileLimit: 2016
sparkHistoryDns: localhost
gatewayDns: localhost
sparkHistoryUrl: http://localhost:8088
allowedUsers:
  - '*'
blockedUsers: []
queues:
  - name: poc
    maxRunningMillis: 21600000
queueTokenSOPS: {}
dbStorageSOPS:
  connectionString: jdbc:postgresql://${dbHostPort}/bpg?useUnicode=yes&characterEncoding=UTF-8&useLegacyDatetimeCode=false&connectTimeout=10000&socketTimeout=30000
  user: bpg
  password: samplepass
statusCacheExpireMillis: 9000
server:
  applicationConnectors:
    - type: http
      port: 8080
logging:
  level: INFO
  loggers:
    com.apple.spark: INFO
sops: {}
EOF
