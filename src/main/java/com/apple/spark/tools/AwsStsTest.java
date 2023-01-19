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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.*;
import com.apple.spark.core.AwsCredentialSparkConfigProvider;

/***
 * This tool is to demo how to connect to AWS STS and get token
 */
public class AwsStsTest {
  public static void main(String[] args) {
    String stsEndpoint = "sts.amazonaws.com";
    String region = "us-west-2";
    String assumeRoleArn = null;
    boolean useWebIdentity = true;
    int tokenDurationSeconds = 1 * 60 * 60;
    String proxyHost = null;
    int proxyPort = 0;
    String proxyProtocol = null;

    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-sts-endpoint")) {
        stsEndpoint = args[i++];
      } else if (argName.equalsIgnoreCase("-region")) {
        region = args[i++];
      } else if (argName.equalsIgnoreCase("-assume-role-arn")) {
        assumeRoleArn = args[i++];
      } else if (argName.equalsIgnoreCase("-use-web-identity")) {
        useWebIdentity = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-token-duration-seconds")) {
        tokenDurationSeconds = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-proxy-host")) {
        proxyHost = args[i++];
      } else if (argName.equalsIgnoreCase("-proxy-port")) {
        proxyPort = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-proxy-protocol")) {
        proxyProtocol = args[i++];
      } else {
        throw new RuntimeException(String.format("Unsupported argument: %s", argName));
      }
    }

    ClientConfiguration clientConfiguration = new ClientConfiguration();
    if (proxyHost != null && !proxyHost.isEmpty()) {
      clientConfiguration.setProxyHost(proxyHost);
      clientConfiguration.setProxyPort(proxyPort);
      if ("http".equalsIgnoreCase(proxyProtocol)) {
        clientConfiguration.setProxyProtocol(Protocol.HTTP);
      } else if ("https".equalsIgnoreCase(proxyProtocol)) {
        clientConfiguration.setProxyProtocol(Protocol.HTTPS);
      } else {
        throw new RuntimeException(String.format("Unsupported http protocol: %s", proxyProtocol));
      }
    }

    AWSSecurityTokenService stsClient =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withRegion(region)
            // .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(stsEndpoint,
            // region))
            .withClientConfiguration(clientConfiguration)
            .build();

    GetCallerIdentityRequest getCallerIdentityRequest = new GetCallerIdentityRequest();
    GetCallerIdentityResult getCallerIdentityResult =
        stsClient.getCallerIdentity(getCallerIdentityRequest);
    System.out.println(String.format("Current Account: %s", getCallerIdentityResult.getAccount()));
    System.out.println(String.format("Current UserId: %s", getCallerIdentityResult.getUserId()));
    System.out.println(String.format("Current ARN: %s", getCallerIdentityResult.getArn()));

    if (assumeRoleArn != null && !assumeRoleArn.isEmpty()) {
      if (useWebIdentity) {
        String webIdentityToken =
            AwsCredentialSparkConfigProvider.getLocalWebIdentityToken(
                AwsCredentialSparkConfigProvider.WEB_IDENTITY_TOKEN_FILE_ENV_VAR_NAME);
        AssumeRoleWithWebIdentityRequest assumeRoleRequest = new AssumeRoleWithWebIdentityRequest();
        assumeRoleRequest.setRoleSessionName("test_assume_role_session_bpg");
        assumeRoleRequest.setRoleArn(assumeRoleArn);
        assumeRoleRequest.setDurationSeconds(tokenDurationSeconds);
        assumeRoleRequest.setWebIdentityToken(webIdentityToken);
        AssumeRoleWithWebIdentityResult assumeRoleResult =
            stsClient.assumeRoleWithWebIdentity(assumeRoleRequest);
        Credentials credentials = assumeRoleResult.getCredentials();
        System.out.println(String.format("AccessKeyId: %s", credentials.getAccessKeyId()));
        System.out.println(String.format("SecretAccessKey: %s", credentials.getSecretAccessKey()));
        System.out.println(String.format("SessionToken: %s", credentials.getSessionToken()));
      } else {
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest();
        assumeRoleRequest.setRoleSessionName("test_assume_role_session_bpg");
        assumeRoleRequest.setRoleArn(assumeRoleArn);
        assumeRoleRequest.setDurationSeconds(tokenDurationSeconds);
        AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);
        Credentials credentials = assumeRoleResult.getCredentials();
        System.out.println(String.format("AccessKeyId: %s", credentials.getAccessKeyId()));
        System.out.println(String.format("SecretAccessKey: %s", credentials.getSecretAccessKey()));
        System.out.println(String.format("SessionToken: %s", credentials.getSessionToken()));
      }
    }
  }
}
