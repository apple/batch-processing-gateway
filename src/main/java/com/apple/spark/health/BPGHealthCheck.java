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

package com.apple.spark.health;

import com.apple.spark.AppConfig;
import com.codahale.metrics.health.HealthCheck;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BPGHealthCheck extends HealthCheck {

  private static final Logger logger = LoggerFactory.getLogger(BPGHealthCheck.class);

  private final List<AppConfig.SparkCluster> sparkClusters;

  public BPGHealthCheck(List<AppConfig.SparkCluster> sparkClusters) {
    this.sparkClusters = sparkClusters;
  }

  @Override
  protected Result check() throws Exception {
    logger.info("healthcheck");

    if (sparkClusters == null || sparkClusters.isEmpty()) {
      return Result.unhealthy("sparkClusters is empty");
    }
    return Result.healthy();
  }
}
