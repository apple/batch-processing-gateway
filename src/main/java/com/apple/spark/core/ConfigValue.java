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

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class ConfigValue {
  private static final Logger logger = LoggerFactory.getLogger(ConfigValue.class);

  private static final String PLAINTEXT_PREFIX = "plaintext:";

  private static final String LOCALHOST_ENV_PREFIX = "localhost:env:";

  private static final String K8S_SECRET_PREFIX = "k8s:secret:";

  private ConfigValue() {}

  /***
   * This method tries to get secret value from an encoded string.
   * Following are examples for encoded string:
   * plaintext:value123 - get plaintext value like value123
   * localhost:env:my_env_variable_name - get value from environment variable my_env_variable_name
   * k8s:secret:namespace1:secret1:key1 - read secret secret1 in current kubernetes cluster and namespace namespace1, and get value for key1
   *
   * If the value is not in recognized encoding, this method will return the value directly.
   * @param value
   * @return
   */
  public static String tryGetEncodedSecretValue(String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    if (value.startsWith(PLAINTEXT_PREFIX)) {
      return value.substring(PLAINTEXT_PREFIX.length());
    } else if (value.startsWith(LOCALHOST_ENV_PREFIX)) {
      return tryGetEncodedSecretValueFromEnvVar(value);
    } else if (value.startsWith(K8S_SECRET_PREFIX)) {
      return tryGetEncodedSecretValueFromK8sSecret(value);
    }
    return value;
  }

  private static String tryGetEncodedSecretValueFromEnvVar(String value) {
    logger.info(String.format("tryGetEncodedSecretValueFromEnvVar: %s", value));
    String envVarName = value.substring(LOCALHOST_ENV_PREFIX.length());
    String envValue = System.getenv(envVarName);
    if (envValue == null) {
      logger.info(String.format("Did not find env variable: %s", envVarName));
      return value;
    } else {
      return envValue;
    }
  }

  private static String tryGetEncodedSecretValueFromK8sSecret(String value) {
    logger.info(String.format("tryGetEncodedSecretValueFromK8sSecret: %s", value));
    String secretNameAndKey = value.substring(K8S_SECRET_PREFIX.length());
    String[] strParts = secretNameAndKey.split(":");
    if (strParts.length != 3) {
      logger.info(String.format("Not valid encoded k8s secret value: %s", value));
      return value;
    }
    String namespace = strParts[0];
    if (namespace.isEmpty()) {
      namespace = KubernetesHelper.tryGetServiceAccountNamespace();
      if (namespace != null && !namespace.isEmpty()) {
        logger.info(
            String.format(
                "Use current namespace %s for encoded k8s secret value: %s", namespace, value));
      }
    }
    if (namespace == null || namespace.isEmpty()) {
      logger.info(String.format("Cannot resolve namespace in encoded k8s secret value: %s", value));
      return value;
    }
    String secretName = strParts[1];
    String secretKey = strParts[2];
    int retryTimes = 2;
    int retryIntervalMillis = 1000;
    for (int i = 0; i <= retryTimes; i++) {
      try (DefaultKubernetesClient client = KubernetesHelper.getLocalK8sClient()) {
        Secret secret = client.secrets().inNamespace(namespace).withName(secretName).get();
        if (secret == null) {
          logger.info(
              String.format(
                  "Secret %s not exists in namespace %s (encoded k8s secret value: %s)",
                  secretName, namespace, value));
          return value;
        }
        logger.info(
            String.format(
                "Found secret %s in namespace %s (encoded k8s secret value: %s)",
                secretName, namespace, value));
        Map<String, String> data = secret.getData();
        if (data == null) {
          logger.info(
              String.format(
                  "Secret %s has no data in namespace %s (encoded k8s secret value: %s)",
                  secretName, namespace, value));
          return value;
        }
        String secretValue = data.get(secretKey);
        if (secretValue == null) {
          logger.info(
              String.format(
                  "Secret %s has no data key %s in namespace %s (encoded k8s secret value: %s)",
                  secretName, secretKey, namespace, value));
          return value;
        } else {
          String base64DecodedStr =
              new String(Base64.getDecoder().decode(secretValue), StandardCharsets.UTF_8);
          logger.info(
              String.format(
                  "Secret %s got value (base64 encoded length: %d, base64 decoded length: %d) for"
                      + " data key %s in namespace %s (encoded k8s secret value: %s)",
                  secretName,
                  secretValue.length(),
                  base64DecodedStr.length(),
                  secretKey,
                  namespace,
                  value));
          return base64DecodedStr;
        }
      } catch (Throwable ex) {
        logger.warn(
            String.format(
                "Failed to get secret %s in namespace %s (encoded k8s secret value: %s)",
                secretName, namespace, value),
            ex);
        try {
          Thread.sleep(retryIntervalMillis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return value;
  }
}
