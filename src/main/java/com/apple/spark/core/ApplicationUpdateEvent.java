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

import com.apple.spark.AppConfig;
import com.apple.spark.operator.SparkApplicationResource;

/** The event generated whenever there's CRD update from an application. */
public class ApplicationUpdateEvent {

  private final SparkApplicationResource prevCRDState;
  private final SparkApplicationResource newCRDState;

  // The runningApplicationMonitor instance corresponding to the specific Spark cluster
  private final RunningApplicationMonitor runningApplicationMonitor;

  // The Spark cluster the CRD update came from
  private final AppConfig.SparkCluster sparkCluster;

  public ApplicationUpdateEvent(
      AppConfig.SparkCluster sparkCluster,
      SparkApplicationResource prevCRDState,
      SparkApplicationResource newCRDState,
      RunningApplicationMonitor runningApplicationMonitor) {
    this.prevCRDState = prevCRDState;
    this.newCRDState = newCRDState;
    this.sparkCluster = sparkCluster;
    this.runningApplicationMonitor = runningApplicationMonitor;
  }

  public SparkApplicationResource getPrevCRDState() {
    return prevCRDState;
  }

  public SparkApplicationResource getNewCRDState() {
    return newCRDState;
  }

  public AppConfig.SparkCluster getSparkCluster() {
    return sparkCluster;
  }

  public RunningApplicationMonitor getRunningApplicationMonitor() {
    return runningApplicationMonitor;
  }
}
