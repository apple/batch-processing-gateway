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
import com.apple.spark.operator.ApplicationState;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationStatus;
import com.apple.spark.util.DateTimeUtils;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.Timer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RunningApplicationMonitorTest {

  @Test
  public void test() throws InterruptedException {
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Timer timer = new Timer(true);
    long interval = 10;
    RunningApplicationMonitor monitor =
        new RunningApplicationMonitor(sparkCluster, timer, interval, new LoggingMeterRegistry());
    Assert.assertEquals(monitor.getApplicationCount(), 0);

    SparkApplication prevCRDState = new SparkApplication();
    SparkApplication newCRDState = new SparkApplication();
    monitor.onUpdate(prevCRDState, newCRDState);
    Assert.assertEquals(monitor.getApplicationCount(), 0);

    newCRDState.getMetadata().setName("app1");
    newCRDState.getMetadata().setNamespace("ns1");
    newCRDState.getMetadata().setCreationTimestamp(DateTimeUtils.format(Instant.now()));
    newCRDState.getMetadata().setLabels(new HashMap<>());
    newCRDState.setStatus(new SparkApplicationStatus());
    newCRDState.getStatus().setApplicationState(new ApplicationState());
    newCRDState.getStatus().getApplicationState().setState("FAILED");
    monitor.onUpdate(prevCRDState, newCRDState);
    Assert.assertEquals(monitor.getApplicationCount(), 0);

    newCRDState.getStatus().getApplicationState().setState("RUNNING");

    monitor.onUpdate(prevCRDState, newCRDState);
    Assert.assertEquals(monitor.getApplicationCount(), 1);

    newCRDState.getMetadata().getLabels().put(Constants.MAX_RUNNING_MILLIS_LABEL, "60000");
    monitor.onUpdate(prevCRDState, newCRDState);
    // sleep for a while to make sure underlying delete check run
    Thread.sleep(interval * 2);
    Assert.assertEquals(monitor.getApplicationCount(), 1);

    newCRDState
        .getMetadata()
        .getLabels()
        .put(Constants.MAX_RUNNING_MILLIS_LABEL, String.valueOf(interval));
    monitor.onUpdate(prevCRDState, newCRDState);
    // sleep for a while to make sure underlying delete check run
    Thread.sleep(interval * 2);
    Assert.assertEquals(monitor.getApplicationCount(), 0);
  }

  @Test
  public void getMaxRunningMillis() {
    SparkApplication sparkApplicationResource = new SparkApplication();
    Assert.assertEquals(
        RunningApplicationMonitor.getMaxRunningMillis(sparkApplicationResource),
        12 * 60 * 60 * 1000);

    sparkApplicationResource.getMetadata().setLabels(new HashMap<>());
    sparkApplicationResource.getMetadata().getLabels().put("maxRunningMillis", "123456789");
    Assert.assertEquals(
        RunningApplicationMonitor.getMaxRunningMillis(sparkApplicationResource), 123456789);
  }

  @Test
  public void exceedMaxRunningTime() throws InterruptedException {
    RunningApplicationMonitor.RunningAppInfo runningAppInfo =
        new RunningApplicationMonitor.RunningAppInfo(System.currentTimeMillis(), 60 * 1000);
    Assert.assertFalse(runningAppInfo.exceedMaxRunningTime());

    runningAppInfo = new RunningApplicationMonitor.RunningAppInfo(System.currentTimeMillis(), 1);
    // sleep for a while to make sure the application exceeds max running time
    Thread.sleep(10);
    Assert.assertTrue(runningAppInfo.exceedMaxRunningTime());
  }
}
