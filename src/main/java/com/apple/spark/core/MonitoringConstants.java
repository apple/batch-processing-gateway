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

public class MonitoringConstants {

  public static final String ENABLE_METRICS_CONF = "enable_metrics";

  // Prometheus monitoring
  public static final String PROMETHEUS_JAR_PATH = "/prometheus/jmx_prometheus_javaagent.jar";
  public static final int DEFAULT_PROMETHEUS_PORT = 9889;
  public static final String DEFAULT_PROMETHEUS_METRICS_PATH = "metrics";

  public static final String DRIVER_CONTAINER_NAME = "spark-kubernetes-driver";
  public static final String EXECUTOR_CONTAINER_NAME = "spark-kubernetes-executor";

  // Datadog autodiscovery annotations
  public static final String DATADOG_AD_PREFIX = "ad.datadoghq.com";
}
