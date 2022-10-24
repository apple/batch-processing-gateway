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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/***
 * This class stores name and a list of tag values for metrics, which could uniquely identify a metric instance.
 */
public class MetricId {

  private final String metricName;
  private final List<String> values;

  public MetricId(String metricName, Collection<String> values) {
    this.metricName = metricName == null ? "" : metricName;
    this.values =
        values == null
            ? Collections.emptyList()
            : values.stream().map(t -> t == null ? "" : t).collect(Collectors.toList());
  }

  public String getMetricName() {
    return metricName;
  }

  public List<String> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricId metricId = (MetricId) o;
    return Objects.equals(metricName, metricId.metricName)
        && Objects.equals(values, metricId.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, values);
  }

  @Override
  public String toString() {
    return "MetricId{" + "metricName='" + metricName + '\'' + ", values=" + values + '}';
  }
}
