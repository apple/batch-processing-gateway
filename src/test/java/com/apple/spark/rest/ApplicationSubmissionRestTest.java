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

package com.apple.spark.rest;

import com.apple.spark.AppConfig;
import com.apple.spark.core.Constants;
import com.codahale.metrics.SharedMetricRegistries;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.HashMap;
import junit.framework.TestCase;
import org.testng.annotations.Test;

public class ApplicationSubmissionRestTest extends TestCase {

  @Test
  public void testGetMaxRuntimeMillis() {
    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("some-queue");
    appConfig.setQueues(Collections.singletonList(queueConfig));
    SharedMetricRegistries.getOrCreate("default");
    ApplicationSubmissionRest submissionRest =
        new ApplicationSubmissionRest(appConfig, new SimpleMeterRegistry());

    // Check for default limit
    HashMap<String, String> sparkConf = new HashMap<>();
    assertEquals(
        Constants.DEFAULT_MAX_RUNNING_MILLIS,
        submissionRest.getMaxRuntimeMillis("some-queue", sparkConf));

    // Check for disabled limit
    queueConfig.setDisableRuntimeLimit(true);
    assertEquals(Long.MAX_VALUE, submissionRest.getMaxRuntimeMillis("some-queue", sparkConf));

    // Check for user overrides
    sparkConf.put(Constants.CONFIG_MAX_RUNNING_MILLIS, "42");
    assertEquals(42L, submissionRest.getMaxRuntimeMillis("some-queue", sparkConf));
  }
}
