package com.apple.spark.appleintegration;

import com.apple.spark.api.*;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.operator.Dependencies;
import com.apple.spark.util.HttpUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.dropwizard.util.Resources;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class IntegrationTestHelper {

  private static final Logger logger = LoggerFactory.getLogger(IntegrationTestHelper.class);
  private static final String authHeaderName = "X-Appleconnect-Acaccountname";
  private static final String authHeaderValue = "skatetest_bot";

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile,
      String mainClass,
      String dependencyPyFile)
      throws IOException {
    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        null);
  }

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile,
      String mainClass,
      String dependencyPyFile,
      String dependencyFile)
      throws IOException {

    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setMainClass(mainClass);

          if (!StringUtils.isEmpty(dependencyPyFile)) {
            if (submitApplicationRequest.getDeps() == null) {
              submitApplicationRequest.setDeps(new Dependencies());
            }
            submitApplicationRequest.getDeps().setPyFiles(Arrays.asList(dependencyPyFile));
          }
          if (!StringUtils.isEmpty(dependencyFile)) {
            if (submitApplicationRequest.getDeps() == null) {
              submitApplicationRequest.setDeps(new Dependencies());
            }
            submitApplicationRequest.getDeps().setFiles(Arrays.asList(dependencyFile));
          }
        });
  }

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    runSparkApplication(
        serviceRootUrl, requestTemplate, requestModifier, SparkConstants.COMPLETED_STATE, false);
  }

  // Returning Driver Log, in case specific test cases want to assert on extra conditions
  public static String runSparkAppAndGetDriverLog(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    return runSparkApplication(
            serviceRootUrl, requestTemplate, requestModifier, SparkConstants.COMPLETED_STATE, true)
        .get();
  }

  public static void runSparkApplicationWhichWillFail(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile)
      throws IOException {
    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
        },
        "FAILED",
        false);
  }

  // When driverLog is true, the result type will be Some<String>, otherwise it would be None.
  private static Optional<String> runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier,
      String expectedFinalState,
      boolean driverLog)
      throws IOException {

    boolean requestTemplateIsYaml = requestTemplate.toLowerCase().endsWith("yaml");

    ObjectMapper objectMapper;
    if (requestTemplateIsYaml) {
      objectMapper =
          new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    } else {
      objectMapper = new ObjectMapper();
    }

    // Use json request file as a request template
    URL resourceUrl = IntegrationTestHelper.class.getResource(requestTemplate);
    String submitApplicationRequestText = Resources.toString(resourceUrl, StandardCharsets.UTF_8);

    // Set application files
    SubmitApplicationRequest submitApplicationRequest =
        objectMapper.readValue(submitApplicationRequestText, SubmitApplicationRequest.class);
    requestModifier.accept(submitApplicationRequest);
    submitApplicationRequestText = objectMapper.writeValueAsString(submitApplicationRequest);

    // submit application
    final String submitUrl = String.format("%s/spark", serviceRootUrl);
    String mediaType;
    if (requestTemplateIsYaml) {
      mediaType = "application/x-yaml";
    } else {
      mediaType = "application/json";
    }
    logger.info("Submitting application to {}", submitUrl);
    SubmitApplicationResponse submitApplicationResponse =
        HttpUtils.post(
            submitUrl,
            submitApplicationRequestText,
            mediaType,
            authHeaderName,
            authHeaderValue,
            SubmitApplicationResponse.class);
    logger.info("Got submission id: {}", submitApplicationResponse.getSubmissionId());
    Assert.assertNotNull(submitApplicationResponse.getSubmissionId());

    String submissionId = submitApplicationResponse.getSubmissionId();

    long startTime = System.currentTimeMillis();

    // query application status
    final String getStatusUrl = String.format("%s/spark/%s/status", serviceRootUrl, submissionId);
    GetSubmissionStatusResponse getSubmissionStatusResponse =
        HttpUtils.get(
            getStatusUrl, authHeaderName, authHeaderValue, GetSubmissionStatusResponse.class);
    logger.info(
        "Application status: {}", objectMapper.writeValueAsString(getSubmissionStatusResponse));

    String applicationState = null;

    while (System.currentTimeMillis() - startTime < 300000) {
      getSubmissionStatusResponse =
          HttpUtils.get(
              getStatusUrl, authHeaderName, authHeaderValue, GetSubmissionStatusResponse.class);
      logger.info(
          "Application status: {}", objectMapper.writeValueAsString(getSubmissionStatusResponse));
      applicationState = getSubmissionStatusResponse.getApplicationState();
      if (SparkConstants.isApplicationStopped(getSubmissionStatusResponse.getApplicationState())) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Assert.assertEquals(applicationState, expectedFinalState);

    // fetch application kube log
    final String getKubeLogUrl =
        String.format("%s/spark/%s/describe", serviceRootUrl, submissionId);
    boolean getKubeLog = false;

    HttpResponse httpResponse =
        HttpUtils.getHttpResponse(getKubeLogUrl, authHeaderName, authHeaderValue);
    logger.info("Reading kube log for submission id: {}", submissionId);
    byte[] buffer = httpResponse.body().toString().getBytes();

    ByteArrayInputStream bufferSource = new ByteArrayInputStream(buffer);
    while (bufferSource.available() > 0) {
      int count = bufferSource.read();
      if (count > 0) {
        getKubeLog = true;
        System.out.write(buffer, 0, count);
      }
    }

    Assert.assertTrue(getKubeLog);

    // fetch application driver log
    String driverLogStr = getDriverLog(serviceRootUrl, submissionId);

    Assert.assertTrue(
        driverLogStr.contains("Created local directory at /mnt/ssd-1")
            || driverLogStr.contains("Created local directory at /mnt/data1-driver")
            || driverLogStr.contains("Created local directory at /mnt/data-driver")
            || driverLogStr.contains("Created local directory at /var/data/"));

    String getDriverUrl = String.format("%s/spark/%s/driver", serviceRootUrl, submissionId);
    GetDriverInfoResponse getDriverInfoResponse =
        HttpUtils.get(getDriverUrl, authHeaderName, authHeaderValue, GetDriverInfoResponse.class);
    Assert.assertNotNull(getDriverInfoResponse.getPodName());
    Assert.assertNotNull(getDriverInfoResponse.getStartTime());

    String getSubmissionsUrl = String.format("%s/spark/mySubmissions", serviceRootUrl);
    GetMySubmissionsResponse getMySubmissionsResponse =
        HttpUtils.get(
            getSubmissionsUrl, authHeaderName, authHeaderValue, GetMySubmissionsResponse.class);
    Assert.assertTrue(
        getMySubmissionsResponse.getSubmissions().stream()
            .anyMatch(t -> t.getSubmissionId().equals(submissionId)));

    String listSubmissionsUrl = String.format("%s/admin/submissions", serviceRootUrl);
    String listSubmissionsResponse =
        HttpUtils.get(listSubmissionsUrl, authHeaderName, authHeaderValue);
    Stream<SubmissionSummary> submissions =
        Arrays.stream(listSubmissionsResponse.split("\\n"))
            .map(
                t -> {
                  try {
                    return objectMapper.readValue(t, SubmissionSummary.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                });
    Assert.assertTrue(submissions.anyMatch(t -> t.getSubmissionId().equals(submissionId)));

    // get submissions by empty application name
    String listSubmissionsUrl2 = String.format("%s/admin/submissions?name=", serviceRootUrl);
    String listSubmissionsResponse2 =
        HttpUtils.get(listSubmissionsUrl2, authHeaderName, authHeaderValue);
    Stream<SubmissionSummary> submissions2 =
        Arrays.stream(listSubmissionsResponse2.split("\\n"))
            .map(
                t -> {
                  try {
                    return objectMapper.readValue(t, SubmissionSummary.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                });
    Assert.assertTrue(submissions2.anyMatch(t -> t.getSubmissionId().equals(submissionId)));

    // get submissions by current application name
    if (submitApplicationRequest.getApplicationName() != null) {
      String encodedApplicationName =
          URLEncoder.encode(
              submitApplicationRequest.getApplicationName(), StandardCharsets.UTF_8.toString());
      String listSubmissionsUrl3 =
          String.format("%s/admin/submissions?name=%s", serviceRootUrl, encodedApplicationName);
      String listSubmissionsResponse3 =
          HttpUtils.get(listSubmissionsUrl3, authHeaderName, authHeaderValue);
      Stream<SubmissionSummary> submissions3 =
          Arrays.stream(listSubmissionsResponse3.split("\\n"))
              .map(
                  t -> {
                    try {
                      return objectMapper.readValue(t, SubmissionSummary.class);
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                    }
                  });
      Assert.assertTrue(submissions3.anyMatch(t -> t.getSubmissionId().equals(submissionId)));
    }

    // get submissions by non existing application name
    String listSubmissionsUrl4 =
        String.format("%s/admin/submissions?name=%s", serviceRootUrl, UUID.randomUUID());
    String listSubmissionsResponse4 =
        HttpUtils.get(listSubmissionsUrl4, authHeaderName, authHeaderValue);
    Assert.assertEquals(listSubmissionsResponse4, "");

    String deleteUrl = String.format("%s/spark/%s", serviceRootUrl, submissionId);
    DeleteSubmissionResponse deleteSubmissionResponse =
        HttpUtils.delete(
            deleteUrl, authHeaderName, authHeaderValue, DeleteSubmissionResponse.class);
    Assert.assertNotNull(deleteSubmissionResponse);
    deleteSubmissionResponse =
        HttpUtils.delete(
            deleteUrl, authHeaderName, authHeaderValue, DeleteSubmissionResponse.class);
    Assert.assertNotNull(deleteSubmissionResponse);

    getMySubmissionsResponse =
        HttpUtils.get(
            getSubmissionsUrl, authHeaderName, authHeaderValue, GetMySubmissionsResponse.class);
    Assert.assertFalse(
        getMySubmissionsResponse.getSubmissions().stream()
            .anyMatch(t -> t.getSubmissionId().equals(submissionId)));

    if (driverLog) {
      return Optional.of(driverLogStr);
    } else {
      return Optional.empty();
    }
  }

  private static String getDriverLog(String serviceRootUrl, String submissionId)
      throws IOException {

    final String getDriverLogUrl = String.format("%s/log?subId=%s", serviceRootUrl, submissionId);

    HttpResponse httpResponse =
        HttpUtils.getHttpResponse(getDriverLogUrl, authHeaderName, authHeaderValue);
    logger.info("Reading driver log for submission id: {}", submissionId);

    byte[] buffer = httpResponse.body().toString().getBytes();

    ByteArrayInputStream bufferSource = new ByteArrayInputStream(buffer);
    while (bufferSource.available() > 0) {
      int count = bufferSource.read();
      if (count > 0) {
        System.out.write(buffer, 0, count);
      }
    }

    return new String(buffer, StandardCharsets.UTF_8);
  }
}
