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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class contains multiple timers metric instances with different name/tags. New metric
 * instance will be created when needed.
 */
public class TimerMetricContainer {

  private final MeterRegistry meterRegistry;
  private final ConcurrentHashMap<MetricId, Timer> timers = new ConcurrentHashMap<>();

  public TimerMetricContainer(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public void record(Runnable runnable, String metricName, Tag... tags) {
    record(runnable, metricName, Arrays.asList(tags));
  }

  public void record(Runnable runnable, String metricName, Collection<Tag> tags) {
    Timer timer = getTimer(metricName, tags);
    timer.record(runnable);
  }

  public <T> T record(Supplier<T> runnable, String metricName, Tag... tags) {
    return record(runnable, metricName, Arrays.asList(tags));
  }

  public <T> T record(Supplier<T> runnable, String metricName, Collection<Tag> tags) {
    Timer timer = getTimer(metricName, tags);
    return timer.record(runnable);
  }

  private Timer getTimer(String metricName, Collection<Tag> tags) {
    List<String> tagValues = tags.stream().map(Tag::getValue).collect(Collectors.toList());
    MetricId metricId = new MetricId(metricName, tagValues);
    Timer timer = timers.get(metricId);
    if (timer == null) {
      timer =
          timers.computeIfAbsent(
              metricId, k -> Timer.builder(metricName).tags(tags).register(meterRegistry));
    }
    return timer;
  }
}
