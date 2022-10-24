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

package com.apple.spark.rest;

import com.amazonaws.regions.Regions;

public class AwsConstants {

  public static final int S3_GET_TIMEOUT_MILLISECS = 180 * 1000;
  public static final int S3_PUT_TIMEOUT_MILLISECS = 180 * 1000;
  public static final Regions CLIENT_REGION = Regions.US_WEST_2;
}
