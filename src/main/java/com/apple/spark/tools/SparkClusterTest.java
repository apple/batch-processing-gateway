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

import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import java.io.File;
import java.io.IOException;

/***
 * This tool is to check a given Spark Cluster
 */
public class SparkClusterTest {
  public static void main(String[] args) throws IOException {
    String apiServer = "";
    String user = "";
    String token = "";
    String caCert = "";
    String httpProxy = null;
    String httpsProxy = null;
    String namespace = "";
    String sparkApplicationFile = "";

    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-api-server")) {
        apiServer = args[i++];
      } else if (argName.equalsIgnoreCase("-user")) {
        user = args[i++];
      } else if (argName.equalsIgnoreCase("-token")) {
        token = args[i++];
      } else if (argName.equalsIgnoreCase("-ca-cert")) {
        caCert = args[i++];
      } else if (argName.equalsIgnoreCase("-http-proxy")) {
        httpProxy = args[i++];
      } else if (argName.equalsIgnoreCase("-https-proxy")) {
        httpsProxy = args[i++];
      } else if (argName.equalsIgnoreCase("-namespace")) {
        namespace = args[i++];
      } else if (argName.equalsIgnoreCase("-spark-application-file")) {
        sparkApplicationFile = args[i++];
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    VirtualSparkClusterSpec sparkCluster = new VirtualSparkClusterSpec();
    sparkCluster.setMasterUrl(apiServer);
    sparkCluster.setUserName(user);
    sparkCluster.setUserTokenSOPS(token);
    sparkCluster.setCaCertDataSOPS(caCert);
    sparkCluster.setHttpProxy(httpProxy);
    sparkCluster.setHttpsProxy(httpsProxy);
    sparkCluster.setSparkApplicationNamespace(namespace);

    System.out.println(
        String.format(
            "Listing pods in cluster %s namespace %s",
            sparkCluster.getMasterUrl(), sparkCluster.getSparkApplicationNamespace()));
    try (KubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster)) {
      PodList podList =
          client.pods().inNamespace(sparkCluster.getSparkApplicationNamespace()).list();
      for (Pod pod : podList.getItems()) {
        System.out.println(
            String.format("Pod %s %s", pod.getMetadata().getName(), pod.getStatus().getPhase()));
      }
    }

    if (sparkApplicationFile != null && !sparkApplicationFile.isEmpty()) {
      System.out.println(
          String.format(
              "Creating Spark Application in cluster %s namespace %s from file %s",
              sparkCluster.getMasterUrl(),
              sparkCluster.getSparkApplicationNamespace(),
              sparkApplicationFile));
      ObjectMapper yamlObjectMapper =
          new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
      SparkApplication sparkApplicationResource =
          yamlObjectMapper.readValue(new File(sparkApplicationFile), SparkApplication.class);
      try (KubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster)) {
        CustomResourceDefinitionContext crdContext =
            KubernetesHelper.getSparkApplicationCrdContext();

        MixedOperation<SparkApplication, SparkApplicationResourceList, Resource<SparkApplication>>
            sparkApplicationClient =
                client.resources(SparkApplication.class, SparkApplicationResourceList.class);

        sparkApplicationClient.create(sparkApplicationResource);

        System.out.println(
            String.format(
                "Created Spark Application in cluster %s namespace %s from file %s",
                sparkCluster.getMasterUrl(),
                sparkCluster.getSparkApplicationNamespace(),
                sparkApplicationFile));
      }
    }
  }
}
