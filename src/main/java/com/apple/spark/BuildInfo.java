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

import java.io.InputStream;
import java.util.Properties;

public class BuildInfo {

  public static String UNKNOWN = "(unknown)";

  public static String Version;
  public static String Revision;

  static {
    String fileName = "build-info.properties";
    try (InputStream resourceStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
      if (resourceStream == null) {
        Version = UNKNOWN;
        Revision = UNKNOWN;
      } else {
        Properties props = new Properties();
        props.load(resourceStream);
        Version = props.getProperty("version", UNKNOWN);
        Revision = props.getProperty("revision", UNKNOWN);
      }
    } catch (Throwable e) {
      Version = UNKNOWN;
      Revision = UNKNOWN;
    }
  }
}
