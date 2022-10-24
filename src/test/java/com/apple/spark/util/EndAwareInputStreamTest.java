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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EndAwareInputStreamTest {

  @Test
  public void read_null_action() throws IOException {
    byte[] data = new byte[] {(byte) 1, (byte) 2};
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    EndAwareInputStream endAwareInputStream = new EndAwareInputStream(inputStream, null);
    Assert.assertEquals(endAwareInputStream.read(), 1);
    Assert.assertEquals(endAwareInputStream.read(), 2);
    Assert.assertEquals(endAwareInputStream.read(), -1);
    Assert.assertEquals(endAwareInputStream.read(new byte[10]), -1);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 10), -1);
  }

  @Test
  public void read_with_action() throws IOException {
    AtomicInteger counter = new AtomicInteger(0);
    Runnable endingAction =
        new Runnable() {
          @Override
          public void run() {
            counter.incrementAndGet();
          }
        };

    byte[] data = new byte[] {(byte) 1, (byte) 2};

    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

    EndAwareInputStream endAwareInputStream = new EndAwareInputStream(inputStream, endingAction);
    Assert.assertEquals(endAwareInputStream.read(), 1);
    Assert.assertEquals(counter.get(), 0);
    Assert.assertEquals(endAwareInputStream.read(), 2);
    Assert.assertEquals(counter.get(), 0);
    Assert.assertEquals(endAwareInputStream.read(), -1);
    Assert.assertEquals(counter.get(), 1);
    Assert.assertEquals(endAwareInputStream.read(new byte[10]), -1);
    Assert.assertEquals(counter.get(), 2);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 10), -1);
    Assert.assertEquals(counter.get(), 3);

    counter.set(0);
    inputStream = new ByteArrayInputStream(data);

    endAwareInputStream = new EndAwareInputStream(inputStream, endingAction);
    Assert.assertEquals(endAwareInputStream.read(new byte[10]), 2);
    Assert.assertEquals(counter.get(), 0);
    Assert.assertEquals(endAwareInputStream.read(new byte[10]), -1);
    Assert.assertEquals(counter.get(), 1);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 10), -1);
    Assert.assertEquals(counter.get(), 2);

    counter.set(0);
    inputStream = new ByteArrayInputStream(data);

    endAwareInputStream = new EndAwareInputStream(inputStream, endingAction);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 1), 1);
    Assert.assertEquals(counter.get(), 0);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 10), 1);
    Assert.assertEquals(counter.get(), 0);
    Assert.assertEquals(endAwareInputStream.read(new byte[10], 0, 10), -1);
    Assert.assertEquals(counter.get(), 1);
  }
}
