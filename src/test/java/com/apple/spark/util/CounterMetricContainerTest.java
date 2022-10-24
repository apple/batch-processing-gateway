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

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CounterMetricContainerTest {

  @Test
  public void test() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    CounterMetricContainer container = new CounterMetricContainer(meterRegistry);
    container.increment("metric1", Tag.of("tag1", "value1"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 1);
    container.increment("metric1", Tag.of("tag1", "value1"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 1);
    container.increment("metric1", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 2);
    container.increment("metric1", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 2);
    container.increment("metric2", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 3);
    container.increment("metric2", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"));
    Assert.assertEquals(meterRegistry.getMeters().size(), 3);
    container.increment(
        "metric2", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"), Tag.of("tag3", ""));
    Assert.assertEquals(meterRegistry.getMeters().size(), 4);
    container.increment(
        "metric2", Tag.of("tag1", "value1"), Tag.of("tag2", "value2"), Tag.of("tag3", ""));
    Assert.assertEquals(meterRegistry.getMeters().size(), 4);
  }
}
