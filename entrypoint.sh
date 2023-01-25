#!/bin/bash

max_heap_size=${MAX_HEAP_SIZE:--Xmx20G}
config_file_path=${CONFIG_FILE_PATH:-/etc/app_config/app-config.yaml}
gz_file_path=${GZ_FILE_PATH:-/app/app-config.gz}
app_monitor_enabled=${APP_MONITOR_ENABLED:-false}

monitor_app_property="-DmonitorApplication=true"

dir=$(dirname $config_file_path)
mkdir -p ${dir}

# Uncompress /app/app-config.gz to /etc/app_config/app-config.yaml if the gz file exists
if [ -f $gz_file_path ]; then
  gunzip -c $gz_file_path > $config_file_path
fi

JAVA_OPT="${max_heap_size} \
-Dkubernetes.auth.tryServiceAccount=false \
-Dkubernetes.auth.tryKubeConfig=false \
-Dcom.sun.management.jmxremote.port=19081 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote=true \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.rmi.port=19081" \

if [ "${app_monitor_enabled,,}" == "true" ]; then
   JAVA_OPT="${JAVA_OPT} ${monitor_app_property}"
fi

# Start Application
java $JAVA_OPT -cp target/bpg-release.jar com.apple.spark.BPGApplication server $config_file_path
