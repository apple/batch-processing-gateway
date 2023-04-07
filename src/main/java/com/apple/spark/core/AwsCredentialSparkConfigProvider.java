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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.apple.spark.AppConfig;
import com.apple.spark.operator.SparkApplicationSpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This class provides spark conf for AWS credentials, e.g.
 * spark.hadoop.fs.s3a.aws.credentials.provider
 * spark.hadoop.fs.s3a.access.key
 * spark.hadoop.fs.s3a.secret.key
 * spark.hadoop.fs.s3a.session.token
 */
public class AwsCredentialSparkConfigProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(AwsCredentialSparkConfigProvider.class);

  public static final String WEB_IDENTITY_TOKEN_FILE_ENV_VAR_NAME = "AWS_WEB_IDENTITY_TOKEN_FILE";

  private AwsCredentialSparkConfigProvider() {}

  /***
   * This method adds aws credential related spark config if needed.
   * @param sparkSpec
   * @param queueConfig this may be null
   */
  public static void addAwsCredentialSparkConfig(
      SparkApplicationSpec sparkSpec, AppConfig.QueueConfig queueConfig) {
    if (sparkSpec.getSparkConf() == null) {
      return;
    }

    String assumeRoleWithWebIdentityRoleArn =
        sparkSpec.getSparkConf().get(Constants.ASSUME_ROLE_WITH_WEB_IDENTITY_ROLE_ARN);
    if (assumeRoleWithWebIdentityRoleArn == null || assumeRoleWithWebIdentityRoleArn.isEmpty()) {
      if (queueConfig != null) {
        assumeRoleWithWebIdentityRoleArn = queueConfig.getDefaultAssumeRole();
      }
    }

    if (assumeRoleWithWebIdentityRoleArn == null || assumeRoleWithWebIdentityRoleArn.isEmpty()) {
      return;
    }

    if (queueConfig == null) {
      throw new RuntimeException("Queue is not configured to allow assuming role");
    }

    if (!queueConfig.allowAssumeRole(assumeRoleWithWebIdentityRoleArn)) {
      throw new RuntimeException(
          String.format(
              "Queue %s does not allow assuming role %s",
              queueConfig.getName(), assumeRoleWithWebIdentityRoleArn));
    }

    logger.info(
        "Adding spark config due to assuming role with web identity for role arn: {}",
        assumeRoleWithWebIdentityRoleArn);

    int durationSeconds = 0;
    String durationSecondsStr =
        sparkSpec.getSparkConf().get(Constants.ASSUME_ROLE_WITH_WEB_IDENTITY_DURATION_SECONDS);
    if (durationSecondsStr != null && !durationSecondsStr.isEmpty()) {
      durationSeconds = Integer.parseInt(durationSecondsStr);
    }

    String webIdentityEnvVarName =
        sparkSpec
            .getSparkConf()
            .get(Constants.ASSUME_ROLE_WITH_WEB_IDENTITY_WEB_IDENTITY_ENV_VAR_NAME);
    if (webIdentityEnvVarName == null || webIdentityEnvVarName.isEmpty()) {
      webIdentityEnvVarName = WEB_IDENTITY_TOKEN_FILE_ENV_VAR_NAME;
    }
    String webIdentityToken = getLocalWebIdentityToken(webIdentityEnvVarName);

    AWSSecurityTokenService stsClient =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withRegion(Regions.getCurrentRegion().getName())
            .build();

    AssumeRoleWithWebIdentityRequest assumeRoleRequest = new AssumeRoleWithWebIdentityRequest();
    assumeRoleRequest.setRoleSessionName(String.format("bgp_assume_role_%s", UUID.randomUUID()));
    assumeRoleRequest.setRoleArn(assumeRoleWithWebIdentityRoleArn);
    assumeRoleRequest.setWebIdentityToken(webIdentityToken);
    if (durationSeconds != 0) {
      assumeRoleRequest.setDurationSeconds(durationSeconds);
    }

    AssumeRoleWithWebIdentityResult assumeRoleResult =
        stsClient.assumeRoleWithWebIdentity(assumeRoleRequest);
    Credentials credentials = assumeRoleResult.getCredentials();
    logger.info(
        "Got AccessKeyId {} after assuming role with web identity for role arn: {}",
        credentials.getAccessKeyId(),
        assumeRoleWithWebIdentityRoleArn);

    sparkSpec
        .getSparkConf()
        .put(
            SparkConstants.SPARK_CONF_S3A_AWS_CREDENTIALS_PROVIDER,
            SparkConstants.SPARK_CONF_S3A_TEMPORARY_AWS_CREDENTIALS_PROVIDER);
    sparkSpec
        .getSparkConf()
        .put(SparkConstants.SPARK_CONF_S3A_ACCESS_KEY, credentials.getAccessKeyId());
    sparkSpec
        .getSparkConf()
        .put(SparkConstants.SPARK_CONF_S3A_SECRET_KEY, credentials.getSecretAccessKey());
    sparkSpec
        .getSparkConf()
        .put(SparkConstants.SPARK_CONF_S3A_SESSION_TOKEN, credentials.getSessionToken());
  }

  public static String getLocalWebIdentityToken(String envVarName) {
    String webIdentityTokenFile = System.getenv(envVarName);
    if (webIdentityTokenFile == null || webIdentityTokenFile.isEmpty()) {
      throw new RuntimeException(String.format("Did not find env variable: %s", envVarName));
    }
    String webIdentityToken;
    try {
      webIdentityToken = Files.readString(Paths.get(webIdentityTokenFile));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to read web identity file: %s", webIdentityTokenFile));
    }
    if (webIdentityToken == null || webIdentityToken.isEmpty()) {
      throw new RuntimeException(
          String.format("Got empty token from web identity file: %s", webIdentityTokenFile));
    }
    return webIdentityToken;
  }
}
