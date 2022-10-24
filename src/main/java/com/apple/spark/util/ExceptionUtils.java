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

import com.apple.spark.core.Constants;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

public class ExceptionUtils {

  private static final MetricRegistry registry;
  private static final Meter jsonProcessingExceptionMeter;
  private static final Meter ioExceptionMeter;
  private static final Meter runtimeExceptionMeter;
  private static final Meter exceptionMeter;

  static {
    registry = SharedMetricRegistries.getOrCreate(Constants.DEFAULT_METRIC_REGISTRY);
    jsonProcessingExceptionMeter = registry.meter(Constants.JSON_PROCESSING_EXCEPTION_METER);
    exceptionMeter = registry.meter(Constants.EXCEPTION_METER);
    ioExceptionMeter = registry.meter(Constants.IO_EXCEPTION_METER);
    runtimeExceptionMeter = registry.meter(Constants.RUNTIME_EXCEPTION_METER);
  }

  public static String getExceptionNameAndMessage(Throwable ex) {
    if (ex == null) {
      return "";
    } else {
      return String.format("%s: %s", ex.getClass().getName(), ex.getMessage());
    }
  }

  public static boolean isTooManyRequest(Throwable ex) {
    while (ex != null) {
      if (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("too many request")) {
        return true;
      }
      ex = ex.getCause();
    }
    return false;
  }

  public static void meterJsonProcessingException() {
    jsonProcessingExceptionMeter.mark();
  }

  public static void meterException() {
    exceptionMeter.mark();
  }

  public static void meterIOException() {
    ioExceptionMeter.mark();
  }

  public static void meterRuntimeException() {
    runtimeExceptionMeter.mark();
  }
}
