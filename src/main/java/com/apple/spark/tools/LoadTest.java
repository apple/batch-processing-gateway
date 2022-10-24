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

package com.apple.spark.tools;

import com.apple.spark.api.GetDriverInfoResponse;
import com.apple.spark.api.GetSubmissionStatusResponse;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.api.SubmitApplicationResponse;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.util.HttpUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadTest {

  private static final Logger logger = LoggerFactory.getLogger(LoadTest.class);

  private static final String serviceRootUrl = "";
  private static final String submitSparkApplicationUrl = String.format("%s/spark", serviceRootUrl);
  private static final String getStatusUrlFormat =
      String.format("%s/spark/%%s/status", serviceRootUrl);
  private static final String getDriverUrlFormat =
      String.format("%s/spark/%%s/driver", serviceRootUrl);
  private static final String deleteUrlFormat = String.format("%s/spark/%%s", serviceRootUrl);
  private static final String authHeaderName = "Authorization";
  private static final String authHeaderValue = "Basic dXNlcjE6cGFzc3dvcmQ=";

  private static final ConcurrentLinkedQueue<TestApplicationInfo> runningApps =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<TestApplicationInfo> finishedApps =
      new ConcurrentLinkedQueue<>();

  public static void main(String[] args) {
    int applications = 10;
    int mapTasks = 2;
    int sleepSeconds = 3;
    int maxWaitSeconds = 60 * 60;
    int executors = 1;
    boolean deleteApplication = true;

    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-applications")) {
        applications = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-mapTasks")) {
        mapTasks = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-sleepSeconds")) {
        sleepSeconds = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxWaitSeconds")) {
        maxWaitSeconds = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-executors")) {
        executors = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-deleteApplication")) {
        deleteApplication = Boolean.parseBoolean(args[i++]);
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    for (int i = 0; i < applications; i++) {
      final int applicationIndex = i;
      final int mapTasksFinal = mapTasks;
      final int sleepSecondsFinal = sleepSeconds;
      final int executorsFinal = executors;
      CompletableFuture.runAsync(
          () ->
              submitApplication(
                  applicationIndex, mapTasksFinal, sleepSecondsFinal, executorsFinal));
    }

    long waitStartTime = System.currentTimeMillis();
    while (finishedApps.size() < applications) {
      if (System.currentTimeMillis() - waitStartTime > TimeUnit.MINUTES.toMillis(maxWaitSeconds)) {
        break;
      }
      List<TestApplicationInfo> appsToRemove = new ArrayList<>();
      for (TestApplicationInfo app : runningApps) {
        String getStatusUrl = String.format(getStatusUrlFormat, app.getSubmissionId());
        GetSubmissionStatusResponse getSubmissionStatusResponse =
            HttpUtils.get(
                getStatusUrl, authHeaderName, authHeaderValue, GetSubmissionStatusResponse.class);
        if (SparkConstants.isApplicationStopped(
            getSubmissionStatusResponse.getApplicationState())) {
          app.setState(getSubmissionStatusResponse.getApplicationState());
          app.setCompletedTime(getSubmissionStatusResponse.getTerminationTime());

          String getDriverUrl = String.format(getDriverUrlFormat, app.getSubmissionId());
          GetDriverInfoResponse getDriverInfoResponse =
              HttpUtils.get(
                  getDriverUrl, authHeaderName, authHeaderValue, GetDriverInfoResponse.class);
          app.setDriverStartTime(getDriverInfoResponse.getStartTime());

          logger.info("Test application completed: {}", app);
          appsToRemove.add(app);
        }
      }
      for (TestApplicationInfo app : appsToRemove) {
        runningApps.remove(app);
        finishedApps.add(app);
      }
      if (!runningApps.isEmpty()) {
        int waitSeconds = 10;
        logger.info("Wait {} seconds to check application", waitSeconds);
        try {
          Thread.sleep(waitSeconds * 1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (deleteApplication) {
      for (TestApplicationInfo app : finishedApps) {
        String deleteUrl = String.format(deleteUrlFormat, app.getSubmissionId());
        logger.info("Deleting application {}", app.getSubmissionId());
        HttpUtils.delete(deleteUrl, authHeaderName, authHeaderValue);
      }
    }

    System.exit(0);
  }

  private static void submitApplication(
      int applicationIndex, int mapTasks, int sleepSeconds, int executorCount) {
    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();

    submitApplicationRequest.setSubmissionIdSuffix("loadtest");

    submitApplicationRequest.setType("Python");
    submitApplicationRequest.setSparkVersion("3.2");
    submitApplicationRequest.setMainApplicationFile(""); // S3 path of PySpark application file
    submitApplicationRequest.setArguments(
        Arrays.asList(String.valueOf(mapTasks), String.valueOf(sleepSeconds)));

    DriverSpec driver = new DriverSpec();

    driver.setCores(1);
    driver.setMemory("512m");

    submitApplicationRequest.setDriver(driver);

    ExecutorSpec executor = new ExecutorSpec();
    executor.setCores(1);
    executor.setMemory("512m");
    executor.setInstances(executorCount);

    submitApplicationRequest.setExecutor(executor);

    String submitApplicationRequestJson;
    try {
      submitApplicationRequestJson =
          (new ObjectMapper()).writeValueAsString(submitApplicationRequest);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json for submit application request", e);
    }

    long submitTime = System.currentTimeMillis();

    SubmitApplicationResponse submitApplicationResponse =
        HttpUtils.post(
            submitSparkApplicationUrl,
            submitApplicationRequestJson,
            authHeaderName,
            authHeaderValue,
            SubmitApplicationResponse.class);

    TestApplicationInfo testApp =
        new TestApplicationInfo(submitApplicationResponse.getSubmissionId());
    testApp.setSubmitTime(submitTime);
    testApp.setSubmitLatency(System.currentTimeMillis() - submitTime);

    runningApps.add(testApp);
    logger.info(
        "Submitted application {} (latency {} millis): {}",
        applicationIndex,
        testApp.getSubmitLatency(),
        submitApplicationResponse.getSubmissionId());
  }

  private static class TestApplicationInfo {

    private final String submissionId;

    private long submitTime;
    private long submitLatency;

    private String state;

    private Long driverStartTime;
    private Long completedTime;

    public TestApplicationInfo(String submissionId) {
      this.submissionId = submissionId;
    }

    public String getSubmissionId() {
      return submissionId;
    }

    public long getSubmitTime() {
      return submitTime;
    }

    public void setSubmitTime(long submitTime) {
      this.submitTime = submitTime;
    }

    public long getSubmitLatency() {
      return submitLatency;
    }

    public void setSubmitLatency(long submitLatency) {
      this.submitLatency = submitLatency;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public Long getDriverStartTime() {
      return driverStartTime;
    }

    public void setDriverStartTime(Long driverStartTime) {
      this.driverStartTime = driverStartTime;
    }

    public Long getCompletedTime() {
      return completedTime;
    }

    public void setCompletedTime(Long completedTime) {
      this.completedTime = completedTime;
    }

    public String getDriverStartLatencySeconds() {
      if (driverStartTime == null) {
        return "(unknown)";
      }
      return String.valueOf((int) Math.round((driverStartTime - submitTime) / 1000.0));
    }

    public String getApplicationFinishLatency() {
      if (completedTime == null) {
        return "(unknown)";
      }
      return String.valueOf((int) Math.round((completedTime - submitTime) / 1000.0));
    }

    @Override
    public String toString() {
      return "TestApp{"
          + "submissionId='"
          + submissionId
          + '\''
          + ", submitTime="
          + submitTime
          + ", submitLatencyMillis="
          + submitLatency
          + ", state='"
          + state
          + '\''
          + ", completedTime="
          + completedTime
          + ", driverStartLatencySeconds="
          + getDriverStartLatencySeconds()
          + ", applicationFinishLatency="
          + getApplicationFinishLatency()
          + '}';
    }
  }
}
