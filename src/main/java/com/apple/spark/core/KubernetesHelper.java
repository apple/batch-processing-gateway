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
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KubernetesHelper {

  // https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/
  public static final String SERVICE_ACCOUNT_FOLDER =
      "/var/run/secrets/kubernetes.io/serviceaccount";
  public static final String SERVICE_ACCOUNT_NAMESPACE_FILE = "namespace";
  public static final String SERVICE_ACCOUNT_CA_CERT_FILE = "ca.crt";
  public static final String SERVICE_ACCOUNT_TOKEN_FILE = "token";

  public static final String LOCAL_API_SERVER_URL = "https://kubernetes.default.svc";

  // https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
  public static final int MAX_LABEL_VALUE_LENGTH = 63;

  private static final Logger logger = LoggerFactory.getLogger(KubernetesHelper.class);

  private static final long DEFAULT_TIMEOUT_MILLIS = 30000;

  public static String tryGetServiceAccountNamespace() {
    ConfigBuilder configBuilder = new ConfigBuilder(Config.autoConfigure(null));
    String namespace = configBuilder.getNamespace();
    if (namespace != null && !namespace.isEmpty()) {
      return namespace;
    }
    Path path = Paths.get(SERVICE_ACCOUNT_FOLDER, SERVICE_ACCOUNT_NAMESPACE_FILE);
    try {
      return Files.readString(path);
    } catch (Throwable e) {
      return "";
    }
  }

  public static String tryGetServiceAccountCACertFile() {
    Path path = Paths.get(SERVICE_ACCOUNT_FOLDER, SERVICE_ACCOUNT_CA_CERT_FILE).toAbsolutePath();
    if (Files.exists(path)) {
      return path.toString();
    } else {
      return "";
    }
  }

  public static String tryGetServiceAccountToken() {
    Path path = Paths.get(SERVICE_ACCOUNT_FOLDER, SERVICE_ACCOUNT_TOKEN_FILE);
    try {
      return Files.readString(path);
    } catch (Throwable e) {
      return "";
    }
  }

  // Note: the client returned by this method may not last for long time, since its credential may
  // expire. Caller of this method should do retry in case the credential expires.
  public static DefaultKubernetesClient getLocalK8sClient() {
    Long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    ConfigBuilder configBuilder =
        new ConfigBuilder(Config.autoConfigure(null))
            .withApiVersion("v1")
            .withTrustCerts(false)
            .withConnectionTimeout(timeoutMillis.intValue())
            .withRequestTimeout(timeoutMillis.intValue())
            .withWebsocketTimeout(timeoutMillis)
            .withUserAgent(Constants.KUBERNETES_USER_AGENT);
    if (configBuilder.getMasterUrl() == null || configBuilder.getMasterUrl().isEmpty()) {
      configBuilder = configBuilder.withMasterUrl(LOCAL_API_SERVER_URL);
    }
    if ((configBuilder.getCaCertFile() == null || configBuilder.getCaCertFile().isEmpty())
        && (configBuilder.getCaCertData() == null || configBuilder.getCaCertData().isEmpty())) {
      String caCertFile = tryGetServiceAccountCACertFile();
      if (caCertFile != null && !caCertFile.isEmpty()) {
        configBuilder = configBuilder.withCaCertFile(caCertFile);
      }
    }
    if (configBuilder.getOauthToken() == null || configBuilder.getOauthToken().isEmpty()) {
      String token = tryGetServiceAccountToken();
      if (token != null && !token.isEmpty()) {
        configBuilder = configBuilder.withOauthToken(token);
      }
    }
    Config config = configBuilder.build();
    return new DefaultKubernetesClient(config);
  }

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
    PodResource podResource = client.pods().inNamespace(namespace).withName(podName);
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
    String userToken = sparkCluster.getUserTokenSOPSDecoded();
    String caCertData = sparkCluster.getCaCertDataSOPSDecoded();
    ConfigBuilder configBuilder =
        new ConfigBuilder()
            .withApiVersion("v1")
            .withMasterUrl(sparkCluster.getMasterUrl())
            .withUsername(sparkCluster.getUserName())
            .withOauthToken(userToken)
            .withCaCertData(caCertData)
            .withNamespace(sparkCluster.getSparkApplicationNamespace())
            .withConnectionTimeout(timeoutMillis.intValue())
            .withRequestTimeout(timeoutMillis.intValue())
            .withWebsocketTimeout(timeoutMillis)
            .withUserAgent(Constants.KUBERNETES_USER_AGENT);
    if (sparkCluster.getHttpProxy() != null && !sparkCluster.getHttpProxy().isEmpty()) {
      configBuilder = configBuilder.withHttpProxy(sparkCluster.getHttpProxy());
    }
    if (sparkCluster.getHttpsProxy() != null && !sparkCluster.getHttpsProxy().isEmpty()) {
      configBuilder = configBuilder.withHttpsProxy(sparkCluster.getHttpsProxy());
    }
    Config config = configBuilder.build();
    logger.debug(String.format("Spark cluster userName %s", sparkCluster.getUserName()));
    logger.debug(
        String.format("Spark cluster userToken first 10 char %s", userToken.substring(0, 10)));
    logger.debug(String.format("Spark cluster userToken length %s", userToken.length()));
    logger.debug(String.format("Spark cluster caCertData length %s", caCertData.length()));
    return config;
  }

  private static boolean isLabelValueAlphanumericChar(char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
  }

  private static boolean isLabelValueChar(char ch) {
    return isLabelValueAlphanumericChar(ch) || ch == '-' || ch == '_' || ch == '.';
  }
}
