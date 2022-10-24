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

package com.apple.spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricIdTest {

  @Test
  public void test() {
    MetricId combination = new MetricId(null, null);
    Assert.assertEquals(combination.getMetricName(), "");
    Assert.assertEquals(combination.getValues().size(), 0);
    Assert.assertEquals(combination, new MetricId(null, null));
    Assert.assertEquals(combination, new MetricId("", new ArrayList<>()));

    combination = new MetricId("", new ArrayList<>());
    Assert.assertEquals(combination.getMetricName(), "");
    Assert.assertEquals(combination.getValues().size(), 0);
    Assert.assertEquals(combination, new MetricId(null, null));
    Assert.assertEquals(combination, new MetricId("", new ArrayList<>()));

    combination = new MetricId("metric1", Arrays.asList("abc"));
    Assert.assertEquals(combination.getMetricName(), "metric1");
    Assert.assertEquals(combination.getValues(), Arrays.asList("abc"));
    Assert.assertEquals(combination, new MetricId("metric1", Arrays.asList("abc")));
    Assert.assertNotEquals(combination, new MetricId("metric1", new ArrayList<>()));
    Assert.assertNotEquals(combination, new MetricId("", Arrays.asList("abc")));

    combination = new MetricId("metric2", Arrays.asList("abc", null, "", "123"));
    Assert.assertEquals(combination.getMetricName(), "metric2");
    Assert.assertEquals(combination.getValues(), Arrays.asList("abc", "", "", "123"));
    Assert.assertEquals(combination, new MetricId("metric2", Arrays.asList("abc", "", "", "123")));
    Assert.assertNotEquals(
        combination, new MetricId("metric", Arrays.asList("abc", "", "", "123")));
    Assert.assertNotEquals(combination, new MetricId("metric2", Arrays.asList("abc", "", "123")));
  }
}
