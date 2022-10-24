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

public class SparkConstants {

  public static final String CLUSTER_MODE = "cluster";

  public static final String SPARK_APP_NAME_CONFIG = "spark.app.name";

  public static final String COMPLETED_STATE = "COMPLETED";
  public static final String RUNNING_STATE = "RUNNING";
  public static final String FAILED_STATE = "FAILED";
  public static final String SUBMITTED_STATE = "SUBMITTED";
  public static final String FAILED_SUBMISSION_STATE = "FAILED_SUBMISSION";
  public static final String UNKNOWN_PHASE = "Unknown";
  public static final String PENDING_PHASE = "Pending";
  public static final String CRD_SCOPE = "Namespaced";
  public static final String CRD_VERSION = "v1beta2";
  public static final String SPARK_APP_NAME_LABEL = "spark.app.name";
  public static final String SPARK_APPLICATION_CRD_GROUP = "sparkoperator.k8s.io";
  public static final String SPARK_APPLICATION_CRD_PLURAL = "sparkapplications";
  public static final String SPARK_APPLICATION_KIND = "SparkApplication";
  public static final String SPARK_OPERATOR_API_VERSION = "sparkoperator.k8s.io/v1beta2";

  public static final Double OVERHEAD_FACTOR_DEFAULT = 0.1;
  public static final Double OVERHEAD_FACTOR_PYSPARK_DEFAULT = 0.4;
  public static final Integer CORE_LIMIT_RATIO = 1000;
  public static final Integer NUM_EXECUTORS_DEFAULT = 1;
  public static final Integer DRIVER_CORE_DEFAULT = 1;
  public static final String DRIVER_MEM_DEFAULT = "1g";
  public static final Integer EXECUTOR_CORE_DEFAULT = 1;
  public static final String EXECUTOR_MEM_DEFAULT = "1g";
  public static final double DRIVER_MEM_BUFFER_RATIO = 1.0;
  public static final double EXECUTOR_MEM_BUFFER_RATIO = 1.0;
  public static final double DRIVER_CPU_BUFFER_RATIO = 1.0;
  public static final double EXECUTOR_CPU_BUFFER_RATIO = 1.0;

  public static final String SPARK_CONF_EXECUTOR_INSTANCES = "spark.executor.instances";

  public static boolean isApplicationStopped(String state) {
    return COMPLETED_STATE.equalsIgnoreCase(state)
        || FAILED_STATE.equalsIgnoreCase(state)
        || FAILED_SUBMISSION_STATE.equalsIgnoreCase(state);
  }
}
