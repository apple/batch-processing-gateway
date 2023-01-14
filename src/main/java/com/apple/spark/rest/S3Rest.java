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

package com.apple.spark.rest;

import static com.apple.spark.core.Constants.S3_API;
import static com.apple.spark.rest.AwsConstants.CLIENT_REGION;
import static com.apple.spark.rest.AwsConstants.S3_PUT_TIMEOUT_MILLISECS;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.apple.spark.AppConfig;
import com.apple.spark.api.UploadS3Response;
import com.apple.spark.security.User;
import com.apple.spark.util.ExceptionUtils;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.security.PermitAll;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PermitAll
@Path(S3_API)
@Produces(MediaType.APPLICATION_JSON)
public class S3Rest extends RestBase {

  private static final Logger logger = LoggerFactory.getLogger(S3Rest.class);

  private static final Optional<String> s3endpoint =
      Optional.ofNullable(System.getProperty("s3.endpoint.url"));

  public S3Rest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);
  }

  @POST
  @Path("{name}")
  @Timed
  @ExceptionMetered(
      name = "WebApplicationException",
      absolute = true,
      cause = WebApplicationException.class)
  @Operation(
      summary = "Upload an artifact to S3 and save as a S3 object.",
      description =
          "The content length parameter must be exactly same as the size of the stream. DO NOT"
              + " upload any sensitive data file.",
      tags = {"Storage"})
  @RequestBody(content = @Content(mediaType = "application/octet-stream"))
  public UploadS3Response uploadStream(
      @Parameter(
              description = "name of the artifact without parent folders",
              example = "artifact.jar")
          @PathParam("name")
          String fileName,
      InputStream inputStream,
      @Parameter(description = "content length can be provided in either query or header")
          @QueryParam("content-length")
          Long contentLength,
      @Parameter(description = "content length can be provided in either query or header")
          @HeaderParam("content-length")
          String contentLengthHeaderValue,
      @Parameter(
              description = "the folder path to which you want to publish the artifact",
              example = "your/folder/")
          @QueryParam("folder")
          String folderName,
      @Parameter(hidden = true) @DefaultValue("none") @HeaderParam("Client-Version")
          String clientVersion,
      @Parameter(hidden = true) @Auth User user) {
    logger.info(
        "LogClientInfo: user {}, {}, Client-Version {}",
        user.getName(),
        "uploadStream",
        clientVersion);
    requestCounters.increment(
        REQUEST_METRIC_NAME, Tag.of("name", "upload_s3_file"), Tag.of("user", user.getName()));

    if (contentLength == null) {
      if (contentLengthHeaderValue == null) {
        throw new WebApplicationException(
            "Please provide content-length in http request header or query parameter",
            Response.Status.BAD_REQUEST);
      }
      try {
        contentLength = Long.parseLong(contentLengthHeaderValue);
      } catch (Throwable ex) {
        throw new WebApplicationException(
            "Please provide valid value for content-length in http request header",
            Response.Status.BAD_REQUEST);
      }
    }

    String key = generateS3Key(fileName, folderName);
    logger.info("Upload file to s3 {} in bucket {}", key, appConfig.getS3Bucket());

    // Use default credential provider chain for imdsv2-rbacv2
    DefaultAWSCredentialsProviderChain awsDefaultCredentialChain =
        new DefaultAWSCredentialsProviderChain();

    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setConnectionTimeout(S3_PUT_TIMEOUT_MILLISECS);
    clientConfiguration.setRequestTimeout(S3_PUT_TIMEOUT_MILLISECS);
    clientConfiguration.setSocketTimeout(S3_PUT_TIMEOUT_MILLISECS);
    clientConfiguration.setClientExecutionTimeout(S3_PUT_TIMEOUT_MILLISECS);

    AmazonS3ClientBuilder s3ClientBuilder =
        AmazonS3ClientBuilder.standard()
            .withClientConfiguration(clientConfiguration)
            .withCredentials(awsDefaultCredentialChain);

    if (s3endpoint.isPresent()) {
      s3ClientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(s3endpoint.get(), CLIENT_REGION.getName()));
      s3ClientBuilder.enablePathStyleAccess();
    } else {
      s3ClientBuilder.withRegion(CLIENT_REGION);
    }

    AmazonS3 s3Client = s3ClientBuilder.build();

    // Sets the size threshold in bytes, for when to use multi-part.
    final long multipartUploadThreshold = 10 * 1024 * 1024; // 10MB

    TransferManager transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3Client)
            .withMultipartCopyThreshold(multipartUploadThreshold)
            .withMultipartUploadThreshold(multipartUploadThreshold)
            .build();

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/octet-stream");
    metadata.setContentLength(contentLength);

    PutObjectRequest request =
        new PutObjectRequest(appConfig.getS3Bucket(), key, inputStream, metadata);

    AtomicLong totalTransferredBytes = new AtomicLong(0);

    request.setGeneralProgressListener(
        new ProgressListener() {
          private long lastLogTime = 0;

          @Override
          public void progressChanged(ProgressEvent progressEvent) {
            long count = progressEvent.getBytesTransferred();
            long total = totalTransferredBytes.addAndGet(count);
            long currentTime = System.currentTimeMillis();
            long logInterval = 10000;
            if (currentTime - lastLogTime >= logInterval) {
              logger.info(
                  "S3 upload progress: {}, recent transferred {} bytes, total transferred {}",
                  key,
                  count,
                  total);
              lastLogTime = currentTime;
            }
          }
        });

    // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/best-practices.html
    request.getRequestClientOptions().setReadLimit((int) multipartUploadThreshold + 1);
    request.setSdkRequestTimeout(S3_PUT_TIMEOUT_MILLISECS);
    request.setSdkClientExecutionTimeout(S3_PUT_TIMEOUT_MILLISECS);
    try {
      Upload upload = transferManager.upload(request);
      UploadResult uploadResult = upload.waitForUploadResult();
      logger.info(
          "S3 upload finished: {}, file {} bytes, total transferred {}",
          key,
          contentLength,
          totalTransferredBytes.get());
      if (!uploadResult.getKey().equals(key)) {
        ExceptionUtils.meterRuntimeException();
        throw new RuntimeException(
            String.format("Invalid key in upload result: %s", uploadResult.getKey()));
      }
    } catch (InterruptedException e) {
      throw new WebApplicationException(
          "Failed to upload to s3: " + ExceptionUtils.getExceptionNameAndMessage(e), e);
    } finally {
      transferManager.shutdownNow();
    }

    UploadS3Response response = new UploadS3Response();
    response.setUrl(String.format("s3a://%s/%s", appConfig.getS3Bucket(), key));
    return response;
  }

  private String generateS3Key(String fileName, String folderName) {
    if (StringUtils.isEmpty(folderName)) {
      DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
      String dateStr = formatter.format(Instant.now());
      return String.format(
          "%s/%s/%s/%s", appConfig.getS3Folder(), dateStr, UUID.randomUUID(), fileName);
    } else {
      folderName = StringUtils.strip(folderName, "/");
      return String.format("%s/%s/%s", appConfig.getS3Folder(), folderName, fileName);
    }
  }
}
