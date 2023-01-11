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

import com.apple.spark.AppConfig;
import com.apple.spark.core.KubernetesHelper;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;

/***
 * This tool is to check a given Spark Cluster
 */
public class SparkClusterTest {
    public static void main(String[] args) {
        String apiServer = "";
        String user = "";
        String token = "";
        String caCert = "";
        String namespace = "";

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
            } else if (argName.equalsIgnoreCase("-namespace")) {
                namespace = args[i++];
            }  else {
                throw new RuntimeException(String.format("Unsupported argument: %s", argName));
            }
        }

        AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
        sparkCluster.setMasterUrl(apiServer);
        sparkCluster.setUserName(user);
        sparkCluster.setUserTokenSOPS(token);
        sparkCluster.setCaCertDataSOPS(caCert);
        sparkCluster.setSparkApplicationNamespace(namespace);

        System.out.println(String.format("Listing pods in cluster %s namespace %s", sparkCluster.getMasterUrl(), sparkCluster.getSparkApplicationNamespace()));
        try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster)) {
            PodList podList = client.pods().inNamespace(sparkCluster.getSparkApplicationNamespace()).list();
            for (Pod pod: podList.getItems()) {
                System.out.println(String.format("Pod %s %s", pod.getMetadata().getName(), pod.getStatus().getPhase()));
            }
        }
    }
}
