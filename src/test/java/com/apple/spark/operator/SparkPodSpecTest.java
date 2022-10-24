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

package com.apple.spark.operator;

import static com.apple.spark.core.Constants.DNS_CONFIG_OPTION_NDOTS_NAME;
import static com.apple.spark.core.Constants.DNS_CONFIG_OPTION_NDOTS_VALUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigOption;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkPodSpecTest {

  @Test
  public void testPodSpecSerialization() throws JsonProcessingException {
    SparkPodSpec spec = new SparkPodSpec();
    PodDNSConfig dnsConfig = new PodDNSConfig();
    dnsConfig.setOptions(
        Collections.singletonList(
            new PodDNSConfigOption(DNS_CONFIG_OPTION_NDOTS_NAME, DNS_CONFIG_OPTION_NDOTS_VALUE)));

    spec.setDnsConfig(dnsConfig);
    Assert.assertEquals(
        new ObjectMapper().writeValueAsString(spec),
        "{\"dnsConfig\":{\"options\":[{\"name\":\"ndots\",\"value\":\"1\"}]}}");
  }

  @Test
  public void testPodSpecDeserialization() throws JsonProcessingException {
    SparkPodSpec spec =
        new ObjectMapper()
            .readValue(
                "{\"dnsConfig\":{\"options\":[{\"name\":\"ndots\",\"value\":\"1\"}]}}",
                SparkPodSpec.class);

    Assert.assertEquals(spec.getDnsConfig().getOptions().get(0).getName(), "ndots");
    Assert.assertEquals(spec.getDnsConfig().getOptions().get(0).getValue(), "1");
  }
}
