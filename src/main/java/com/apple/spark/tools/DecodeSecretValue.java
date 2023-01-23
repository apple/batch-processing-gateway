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

package com.apple.spark.tools;

import com.apple.spark.core.ConfigValue;

/***
 * This tool is to decode a secret value like k8s:secret:namespace1:secret1:key1
 */
public class DecodeSecretValue {
  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Please provide an argument");
      return;
    }
    String inputValue = args[0];
    String decodedValue = ConfigValue.tryGetEncodedSecretValue(inputValue);
    System.out.println(String.format("Got decoded value: %s -> %s", inputValue, decodedValue));
  }
}
