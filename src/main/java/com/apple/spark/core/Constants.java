/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Constants {

  // This value will be used as certain prefixes and suffixes
  public static final String SERVICE_ABBR = "bpg";
  public static final String REALM = SERVICE_ABBR;
  public static final String KUBERNETES_USER_AGENT = SERVICE_ABBR;

  public static final String DEFAULT_DB_NAME = SERVICE_ABBR;

  // The header to use as an option to pass in user account
  public static final String USER_HEADER_KEY = "User-Account";

  // Hardcoded Airflow system accounts
  // This is to help BPG identify those apps submitted by Airflow DAGs
  public static final Set<String> AIRFLOW_SYSTEM_ACCOUNTS =
      new HashSet<>(Arrays.asList("airflow_system_account"));

  public static final String UNKNOWN_STATE = "UNKNOWN";
  public static final String FAILED_STATE = "FAILED";
  public static final String SPARK_APPLICATION_RESOURCE_NAME_VAR =
      "{spark-application-resource-name}";
  public static final String PYTHON_TYPE = "Python";
  public static final String JAVA_TYPE = "Java";
  public static final String PROXY_USER_LABEL = "proxyUser";
  public static final String APPLICATION_NAME_LABEL = "applicationName";
  public static final String QUEUE_LABEL = "queue";
  public static final String DAG_NAME_LABEL = "dag_name";
  public static final String TASK_NAME_LABEL = "task_name";
  public static final String COST_TABLE_NAME = "cost";
  public static final String MAX_RUNNING_MILLIS_LABEL = "maxRunningMillis";
  public static final String DEFAULT_DRIVER_NODE_LABEL_KEY = "node_group_category";
  public static final String DEFAULT_EXECUTOR_NODE_LABEL_KEY = "node_group_category";
  public static final long DEFAULT_MAX_RUNNING_MILLIS = TimeUnit.HOURS.toMillis(12);
  public static final String DEFAULT_METRIC_REGISTRY =
      String.format("%s-default-metric-registry", SERVICE_ABBR);
  public static final String JSON_PROCESSING_EXCEPTION_METER = "JsonProcessingException.exceptions";
  public static final String IO_EXCEPTION_METER = "IOException.exceptions";
  public static final String RUNTIME_EXCEPTION_METER = "RuntimeException.exceptions";
  public static final String EXCEPTION_METER = "Exception.exceptions";
  public static final String CONFIG_MAX_RUNNING_MILLIS =
      String.format("%s.maxRunningMillis", SERVICE_ABBR);
  public static final int APPLICATION_MONITOR_QUEUE_CAPACITY = 100000;
  public static final long DEFAULT_STATUS_CACHE_EXPIRE_MILLIS = 990;
  public static final String DRIVER_NODE_AFFINITY_KEY = "node_group_category";
  public static final String DRIVER_NODE_AFFINITY_VALUE = "spark_driver";
  public static final int NODE_AFFINITY_WEIGHT = 100;
  public static final String DRIVER_NODE_SELECTOR_KEY = "node_group_category";
  public static final String DRIVER_NODE_SELECTOR_VALUE = "spark_driver";
  public static final String EXECUTOR_NODE_SELECTOR_KEY = "node_group_category";
  public static final String EXECUTOR_NODE_SELECTOR_VALUE = "spark_executor";
  public static final String SYSTEM_NODE_SELECTOR_KEY = "node_group_category";
  public static final String SYSTEM_NODE_SELECTOR_VALUE = "spark_eks_system";
  public static final String KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_KEY =
      "cluster-autoscaler.kubernetes.io/safe-to-evict";
  public static final String KUBE_CLUSTER_AUTOSCALER_SCALE_IN_ANNOTATION_VALUE = "false";
  public static final String DNS_CONFIG_OPTION_NDOTS_NAME = "ndots";
  public static final String DNS_CONFIG_OPTION_NDOTS_VALUE = "1";
  public static final int MAX_EXECUTOR_INSTANCES = 5000;

  // Counters
  public static final String STATUS_CACHE_LOAD =
      String.format("statsd.%s.status_cache.load", SERVICE_ABBR);
  public static final String STATUS_CACHE_LOAD_FAILURE =
      String.format("statsd.%s.status_cache.load_failure", SERVICE_ABBR);

  public static final String STATUS_CACHE_GET_FAILURE =
      String.format("statsd.%s.status_cache.get_failure", SERVICE_ABBR);

  public static final String MONITOR_RUNNING_APPS =
      String.format("statsd.%s.monitor.running_apps", SERVICE_ABBR);

  public static final String MONITOR_KILLED_APPS =
      String.format("statsd.%s.monitor.killed_apps", SERVICE_ABBR);

  public static final String QUEUE_INFO = String.format("statsd.%s.queue_info", SERVICE_ABBR);

  public static final String APPLICATION_FINISH_METRIC_NAME =
      String.format("statsd.%s.application_finish", SERVICE_ABBR);

  public static final String MONITOR_QUEUE_SIZE =
      String.format("statsd.%s.monitor.queue_size", SERVICE_ABBR);

  public static final String MONITOR_DROPPED_EVENT =
      String.format("statsd.%s.monitor.dropped_event", SERVICE_ABBR);

  public static final String DEFAULT_APPLICATION_CONTEXT_PATH = "/skatev2";

  // API Endpoints
  public static final String SPARK_API = "/spark";
  public static final String S3_API = "/s3";
  public static final String HEALTH_CHECK_API = "/healthcheck";
  public static final String ADMIN_API = "/admin";
  public static final String LOG_API = "/log";
  public static final String HIVE_METASTORE_SASL_ENABLED =
          "spark.hadoop.hive.metastore.sasl.enabled";
}
