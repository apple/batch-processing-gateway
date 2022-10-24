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

package com.apple.spark;

import java.util.ArrayList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GatewayConfigTest {

  @Test
  public void matchSparkVersion() {
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Assert.assertFalse(sparkCluster.matchSparkVersion(null));
    Assert.assertFalse(sparkCluster.matchSparkVersion(""));
    Assert.assertFalse(sparkCluster.matchSparkVersion("2.4"));

    sparkCluster.setSparkVersions(new ArrayList<>());
    Assert.assertFalse(sparkCluster.matchSparkVersion(null));
    Assert.assertFalse(sparkCluster.matchSparkVersion(""));
    Assert.assertFalse(sparkCluster.matchSparkVersion("2.4"));

    sparkCluster.setSparkVersions(Arrays.asList("2.4"));
    Assert.assertFalse(sparkCluster.matchSparkVersion(null));
    Assert.assertFalse(sparkCluster.matchSparkVersion(""));
    Assert.assertTrue(sparkCluster.matchSparkVersion("2.4"));
    Assert.assertFalse(sparkCluster.matchSparkVersion("3.0"));

    sparkCluster.setSparkVersions(Arrays.asList("2.4", "3.0"));
    Assert.assertFalse(sparkCluster.matchSparkVersion(null));
    Assert.assertFalse(sparkCluster.matchSparkVersion(""));
    Assert.assertTrue(sparkCluster.matchSparkVersion("2.4"));
    Assert.assertTrue(sparkCluster.matchSparkVersion("3.0"));
    Assert.assertFalse(sparkCluster.matchSparkVersion("3.1"));
  }

  @Test
  public void matchQueue() {
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Assert.assertFalse(sparkCluster.matchQueue(null));
    Assert.assertFalse(sparkCluster.matchQueue(""));
    Assert.assertFalse(sparkCluster.matchQueue("q1"));

    sparkCluster.setQueues(new ArrayList<>());
    Assert.assertFalse(sparkCluster.matchQueue(null));
    Assert.assertFalse(sparkCluster.matchQueue(""));
    Assert.assertFalse(sparkCluster.matchQueue("q1"));

    sparkCluster.setQueues(Arrays.asList("q1"));
    Assert.assertFalse(sparkCluster.matchQueue(null));
    Assert.assertFalse(sparkCluster.matchQueue(""));
    Assert.assertTrue(sparkCluster.matchQueue("q1"));
    Assert.assertFalse(sparkCluster.matchQueue("q2"));

    sparkCluster.setQueues(Arrays.asList("q1", "q2"));
    Assert.assertFalse(sparkCluster.matchQueue(null));
    Assert.assertFalse(sparkCluster.matchQueue(""));
    Assert.assertTrue(sparkCluster.matchQueue("q1"));
    Assert.assertTrue(sparkCluster.matchQueue("q2"));
    Assert.assertFalse(sparkCluster.matchQueue("q3"));
  }
}
