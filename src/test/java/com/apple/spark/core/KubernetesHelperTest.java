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

public class KubernetesHelperTest {

  @Test
  public void test_normalizeLabelValue() {
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue(null), null);
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue(""), "");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("0"), "0");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("9"), "9");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("a"), "a");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("z"), "z");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("A"), "A");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("Z"), "Z");

    String str1 = new String(new char[KubernetesHelper.MAX_LABEL_VALUE_LENGTH]).replace("\0", "a");
    String str2 = str1 + "1";
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue(str1), str1);
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue(str2), str1);

    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("0aAzZ_-.9"), "0aAzZ_-.9");

    Assert.assertEquals(KubernetesHelper.normalizeLabelValue(" "), "0");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("  "), "00");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("--"), "00");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("__"), "00");
    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("_0aAzZ_-.9_"), "00aAzZ_-.90");

    Assert.assertEquals(KubernetesHelper.normalizeLabelValue("0aAzZ_-.!@ 9"), "0aAzZ_-.0009");
  }
}
