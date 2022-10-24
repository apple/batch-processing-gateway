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

import com.apple.spark.operator.EnvVar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSpecHelper {

  // Copy environment variables from source to destination, with proper overwriting
  public static void copyEnv(List<EnvVar> source, List<EnvVar> destination) {
    Map<String, Integer> destinationEnvIndex = new HashMap<>();
    for (int i = 0; i < destination.size(); i++) {
      destinationEnvIndex.put(destination.get(i).getName(), i);
    }
    for (EnvVar sourceEnvEntry : source) {
      Integer indexInDestinationEnv = destinationEnvIndex.get(sourceEnvEntry.getName());
      if (indexInDestinationEnv == null) {
        destination.add(sourceEnvEntry);
      } else {
        destination.set(indexInDestinationEnv, sourceEnvEntry);
      }
    }
  }
}
