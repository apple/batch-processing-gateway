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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigValueTest {

  @Test
  public void tryGetEncodedSecretValue() {
    String value = ConfigValue.tryGetEncodedSecretValue(null);
    Assert.assertNull(value);

    value = ConfigValue.tryGetEncodedSecretValue("");
    Assert.assertEquals(value, "");

    value = ConfigValue.tryGetEncodedSecretValue("abc123");
    Assert.assertEquals(value, "abc123");

    value = ConfigValue.tryGetEncodedSecretValue("plaintext:abc123");
    Assert.assertEquals(value, "abc123");

    value =
        ConfigValue.tryGetEncodedSecretValue(
            "localhost:env:not_existing_env_variable_b50dd229-00a8-410e-a279-a22e6bad0a4c");
    Assert.assertEquals(
        value, "localhost:env:not_existing_env_variable_b50dd229-00a8-410e-a279-a22e6bad0a4c");

    String envVariableNameForTest = System.getenv().keySet().stream().findFirst().get();
    String envVariableValueForTest = System.getenv(envVariableNameForTest);
    value = ConfigValue.tryGetEncodedSecretValue("localhost:env:" + envVariableNameForTest);
    Assert.assertEquals(value, envVariableValueForTest);

    value = ConfigValue.tryGetEncodedSecretValue("k8s:secret:invalid_value");
    Assert.assertEquals(value, "k8s:secret:invalid_value");
  }
}
