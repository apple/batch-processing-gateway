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

import com.apple.spark.AppConfig;
import com.apple.spark.util.EndAwareInputStream;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import java.io.Closeable;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesHelper {

  // https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
  public static final int MAX_LABEL_VALUE_LENGTH = 63;

  private static final Logger logger = LoggerFactory.getLogger(KubernetesHelper.class);

  private static final long DEFAULT_TIMEOUT_MILLIS = 30000;

  public static DefaultKubernetesClient getK8sClient(AppConfig.SparkCluster sparkCluster) {
    return new DefaultKubernetesClient(getK8sConfig(sparkCluster));
  }

  public static CustomResourceDefinitionContext getSparkApplicationCrdContext() {
    return new CustomResourceDefinitionContext.Builder()
        .withGroup(SparkConstants.SPARK_APPLICATION_CRD_GROUP)
        .withScope(SparkConstants.CRD_SCOPE)
        .withVersion(SparkConstants.CRD_VERSION)
        .withPlural(SparkConstants.SPARK_APPLICATION_CRD_PLURAL)
        .build();
  }

  public static InputStream tryGetLogStream(
      DefaultKubernetesClient client, String namespace, String podName) {
    PodResource<Pod, DoneablePod> podResource =
        client.inNamespace(namespace).pods().withName(podName);
    if (podResource == null) {
      logger.info("Cannot get pod resource {}", podName);
      return null;
    }
    Pod pod = podResource.get();
    if (pod == null) {
      logger.info("Cannot get pod {} from pod resource object", podName);
      return null;
    }
    String podPhase = pod.getStatus().getPhase();
    if (podPhase.equalsIgnoreCase(SparkConstants.UNKNOWN_PHASE)
        || podPhase.equalsIgnoreCase(SparkConstants.PENDING_PHASE)) {
      logger.info("Cannot get log, pod {} in phase {}", podName, podPhase);
      return null;
    }

    LogWatch logWatch = podResource.watchLog();
    if (logWatch == null) {
      logger.info("Log watch is null for pod {}", podName);
      return null;
    }

    InputStream logStream = logWatch.getOutput();
    logger.info("Got log stream for pod {}", podName);

    return new EndAwareInputStream(
        logStream,
        () -> {
          closeQuietly(logWatch);
          closeQuietly(client);
        });
  }

  public static void closeQuietly(Closeable closeable) {
    try {
      closeable.close();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to close %s", closeable), ex);
    }
  }

  // Normalize a string value to make it valid for label value
  // https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
  // Valid label value:
  // must be 63 characters or less (can be empty),
  // unless empty, must begin and end with an alphanumeric character ([a-z0-9A-Z]),
  // could contain dashes (-), underscores (_), dots (.), and alphanumerics between.
  public static String normalizeLabelValue(String str) {
    if (str == null) {
      return str;
    }
    final char replacementChar = '0';
    StringBuilder sb = new StringBuilder();
    for (char ch : str.toCharArray()) {
      if (sb.length() == 0) {
        if (isLabelValueAlphanumericChar(ch)) {
          sb.append(ch);
        } else {
          sb.append(replacementChar);
        }
      } else {
        if (isLabelValueChar(ch)) {
          sb.append(ch);
        } else {
          sb.append(replacementChar);
        }
      }
      if (sb.length() >= MAX_LABEL_VALUE_LENGTH) {
        break;
      }
    }

    String labelValue = sb.toString();
    if (labelValue.isEmpty()) {
      return labelValue;
    }

    char lastChar = labelValue.charAt(labelValue.length() - 1);
    if (!isLabelValueAlphanumericChar(lastChar)) {
      labelValue = labelValue.substring(0, labelValue.length() - 1) + replacementChar;
    }

    return labelValue;
  }

  private static Config getK8sConfig(AppConfig.SparkCluster sparkCluster) {
    Long timeoutMillis = sparkCluster.getTimeoutMillis();
    if (timeoutMillis == null) {
      timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    }
    Config config =
        new ConfigBuilder()
            .withApiVersion("v1")
            .withMasterUrl(sparkCluster.getMasterUrl())
            .withUsername(sparkCluster.getUserName())
            .withOauthToken(sparkCluster.getUserTokenSOPS())
            .withCaCertData(sparkCluster.getCaCertDataSOPS())
            .withNamespace(sparkCluster.getSparkApplicationNamespace())
            .withConnectionTimeout(timeoutMillis.intValue())
            .withRequestTimeout(timeoutMillis.intValue())
            .withWebsocketTimeout(timeoutMillis)
            .withUserAgent(Constants.KUBERNETES_USER_AGENT)
            .build();
    logger.debug(String.format("Spark cluster userName %s", sparkCluster.getUserName()));
    logger.debug(
        String.format(
            "Spark cluster userToken first 10 char %s",
            sparkCluster.getUserTokenSOPS().substring(0, 10)));
    logger.debug(
        String.format(
            "Spark cluster userToken length %s", sparkCluster.getUserTokenSOPS().length()));
    logger.debug(
        String.format(
            "Spark cluster caCertData length %s", sparkCluster.getCaCertDataSOPS().length()));
    return config;
  }

  private static boolean isLabelValueAlphanumericChar(char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
  }

  private static boolean isLabelValueChar(char ch) {
    return isLabelValueAlphanumericChar(ch) || ch == '-' || ch == '_' || ch == '.';
  }
}
