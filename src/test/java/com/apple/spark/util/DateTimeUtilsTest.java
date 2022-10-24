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

import org.testng.Assert;
import org.testng.annotations.Test;

public class DateTimeUtilsTest {

  @Test
  public void parse() {
    Assert.assertEquals(DateTimeUtils.parseOrNull(null), null);
    Assert.assertEquals(DateTimeUtils.parseOrNull(""), null);
    Assert.assertEquals(DateTimeUtils.parseOrNull("2020-11-25"), null);
    Assert.assertEquals(
        DateTimeUtils.parseOrNull("2020-11-25T21:48:56Z"), new Long(1606340936000L));
    Assert.assertEquals(
        DateTimeUtils.parseOrNull("2020-11-25T21:48:56z"), new Long(1606340936000L));
    Assert.assertEquals(
        DateTimeUtils.parseOrNull("2020-11-25T21:48:56.123Z"), new Long(1606340936123L));
    Assert.assertEquals(
        DateTimeUtils.parseOrNull("2020-11-25T21:48:56.123z"), new Long(1606340936123L));
  }
}
