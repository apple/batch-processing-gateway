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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import io.micrometer.statsd.StatsdMeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BPGStatsdConfig implements StatsdConfig {

  private static final Logger logger = LoggerFactory.getLogger(BPGStatsdConfig.class);

  private static final String STATSD_SERVER_IP_ENV_NAME = "STATSD_SERVER_IP";
  private static final String STATSD_SERVER_PORT_ENV_NAME = "STATSD_SERVER_PORT";

  public static MeterRegistry createMeterRegistry() {
    BPGStatsdConfig statsdConfig = new BPGStatsdConfig();
    MeterRegistry meterRegistry;
    if (statsdConfig.enabled()) {
      logger.info("Use statsd meter registry");
      meterRegistry = new StatsdMeterRegistry(statsdConfig, Clock.SYSTEM);
    } else {
      logger.info("Use logging meter registry");
      meterRegistry = new LoggingMeterRegistry();
    }
    return meterRegistry;
  }

  @Override
  public String get(String s) {
    return null;
  }

  @Override
  public boolean enabled() {
    return !StringUtils.isEmpty(host()) && port() > 0;
  }

  @Override
  public StatsdFlavor flavor() {
    return StatsdFlavor.DATADOG;
  }

  @Override
  public String host() {
    return System.getenv(STATSD_SERVER_IP_ENV_NAME);
  }

  @Override
  public int port() {
    String value = System.getenv(STATSD_SERVER_PORT_ENV_NAME);
    if (StringUtils.isEmpty(value)) {
      return 0;
    }
    try {
      return Integer.parseInt(value);
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to parse environment value for %s: %s", STATSD_SERVER_PORT_ENV_NAME, value),
          ex);
      return 0;
    }
  }
}
