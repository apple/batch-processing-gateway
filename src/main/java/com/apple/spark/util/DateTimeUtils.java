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

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeUtils {

  private static final Logger logger = LoggerFactory.getLogger(DateTimeUtils.class);

  public static Long parseOrNull(String str) {
    if (str == null || str.isEmpty()) {
      return null;
    }

    try {
      return Instant.parse(str).toEpochMilli();
    } catch (Throwable ex) {
      logger.debug(String.format("Failed to parse datetime: %s", str), ex);
      return null;
    }
  }

  public static String format(Instant instant) {
    return ISO_INSTANT.format(instant);
  }
}
