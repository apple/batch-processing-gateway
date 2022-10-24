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
import com.apple.spark.operator.EnvVarSource;
import com.apple.spark.operator.ObjectFieldSelector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkSpecHelperTest {

  @Test
  public void copyEnv() {
    List<EnvVar> source = new ArrayList<>();
    List<EnvVar> destination = new ArrayList<>();

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 0);

    source = Arrays.asList(new EnvVar("key1", "value1"), new EnvVar("key2", "value2"));

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 2);
    Assert.assertEquals(destination.get(0).getName(), "key1");
    Assert.assertEquals(destination.get(0).getValue(), "value1");
    Assert.assertEquals(destination.get(0).getValueFrom(), null);
    Assert.assertEquals(destination.get(1).getName(), "key2");
    Assert.assertEquals(destination.get(1).getValue(), "value2");

    source = Arrays.asList(new EnvVar("key1", "value1"), new EnvVar("key2", "value2a"));

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 2);
    Assert.assertEquals(destination.get(0).getName(), "key1");
    Assert.assertEquals(destination.get(0).getValue(), "value1");
    Assert.assertEquals(destination.get(1).getName(), "key2");
    Assert.assertEquals(destination.get(1).getValue(), "value2a");

    source = Arrays.asList(new EnvVar("key1", "value1a"), new EnvVar("key3", "value3"));

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 3);
    Assert.assertEquals(destination.get(0).getName(), "key1");
    Assert.assertEquals(destination.get(0).getValue(), "value1a");
    Assert.assertEquals(destination.get(1).getName(), "key2");
    Assert.assertEquals(destination.get(1).getValue(), "value2a");
    Assert.assertEquals(destination.get(2).getName(), "key3");
    Assert.assertEquals(destination.get(2).getValue(), "value3");

    source = Arrays.asList(new EnvVar("key1", "value1b"), new EnvVar("key1", "value1c"));

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 3);
    Assert.assertEquals(destination.get(0).getName(), "key1");
    Assert.assertEquals(destination.get(0).getValue(), "value1c");
    Assert.assertEquals(destination.get(1).getName(), "key2");
    Assert.assertEquals(destination.get(1).getValue(), "value2a");
    Assert.assertEquals(destination.get(2).getName(), "key3");
    Assert.assertEquals(destination.get(2).getValue(), "value3");

    source =
        Arrays.asList(
            new EnvVar("key1", "value1b"),
            new EnvVar(
                "key1", "value1d", new EnvVarSource(new ObjectFieldSelector("status.hostIP"))));

    SparkSpecHelper.copyEnv(source, destination);
    Assert.assertEquals(destination.size(), 3);
    Assert.assertEquals(destination.get(0).getName(), "key1");
    Assert.assertEquals(destination.get(0).getValue(), "value1d");
    Assert.assertEquals(
        destination.get(0).getValueFrom().getFieldRef().getFieldPath(), "status.hostIP");
    Assert.assertEquals(destination.get(1).getName(), "key2");
    Assert.assertEquals(destination.get(1).getValue(), "value2a");
    Assert.assertEquals(destination.get(2).getName(), "key3");
    Assert.assertEquals(destination.get(2).getValue(), "value3");
  }
}
