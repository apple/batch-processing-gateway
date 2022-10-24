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

public class ExceptionUtilsTest {

  @Test
  public void getExceptionNameAndMessage() {
    Assert.assertEquals(ExceptionUtils.getExceptionNameAndMessage(null), "");
    Assert.assertEquals(
        ExceptionUtils.getExceptionNameAndMessage(new RuntimeException("some error")),
        "java.lang.RuntimeException: some error");
  }

  @Test
  public void isTooManyRequests() {
    Assert.assertFalse(ExceptionUtils.isTooManyRequest(null));
    Assert.assertFalse(ExceptionUtils.isTooManyRequest(new RuntimeException()));
    Assert.assertFalse(ExceptionUtils.isTooManyRequest(new RuntimeException("")));
    Assert.assertTrue(
        ExceptionUtils.isTooManyRequest(new RuntimeException("Too many requests exception")));
    Assert.assertTrue(
        ExceptionUtils.isTooManyRequest(new RuntimeException("exception: Too many requests")));
    Assert.assertFalse(ExceptionUtils.isTooManyRequest(new RuntimeException("", null)));
    Assert.assertFalse(ExceptionUtils.isTooManyRequest(new RuntimeException("ex1", null)));
    Assert.assertTrue(
        ExceptionUtils.isTooManyRequest(
            new RuntimeException("ex1", new RuntimeException("Too many requests exception"))));
    Assert.assertTrue(
        ExceptionUtils.isTooManyRequest(
            new RuntimeException(
                "ex1", new RuntimeException("mixed upper lower case: too Many requests"))));
  }
}
