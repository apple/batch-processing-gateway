package com.apple.spark.appleintegration;

import static com.apple.spark.util.HttpUtils.post;

import com.apple.spark.api.*;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.operator.Dependencies;
import com.apple.spark.util.HttpUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
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
  private static final String authHeaderValue = "raimldpi";

  public static byte[] getResourceAsBytes(String resourceUrl) throws IOException {
    try (InputStream is = IntegrationTestHelper.class.getResourceAsStream(resourceUrl)) {
      return Objects.requireNonNull(is).readAllBytes();
    }
  }

  public static String getResourceAsString(String resourceUrl, Charset charset) throws IOException {
    return new String(getResourceAsBytes(resourceUrl), charset);
  }

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile,
      String mainClass,
      String dependencyPyFile,
      String queue)
      throws IOException {
    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        null,
        queue);
  }

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile,
      String mainClass,
      String dependencyPyFile,
      String dependencyFile,
      String queue)
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
          submitApplicationRequest.setQueue(queue);
        });
  }

  public static void runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        requestModifier,
        SparkConstants.COMPLETED_STATE,
        false,
        false);
  }

  // Returning Driver Log, in case specific test cases want to assert on extra conditions
  public static String runSparkAppAndGetDriverLog(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    return runSparkApplication(
            serviceRootUrl,
            requestTemplate,
            requestModifier,
            SparkConstants.COMPLETED_STATE,
            true,
            false)
        .get();
  }

  public static String runSparkAppAndGetSparkSpec(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    return runSparkApplication(
            serviceRootUrl,
            requestTemplate,
            requestModifier,
            SparkConstants.COMPLETED_STATE,
            false,
            true)
        .get();
  }

  public static void runSparkApplicationWhichWillFail(
      String serviceRootUrl,
      String requestTemplate,
      String sparkVersion,
      String sparkApplicationFile,
      String queue)
      throws IOException {
    runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setQueue(queue);
        },
        "FAILED",
        false,
        false);
  }

  public static String generateSubmitApplicationRequestStr(
      ObjectMapper objectMapper,
      SubmitApplicationRequest submitApplicationRequest,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws IOException {
    requestModifier.accept(submitApplicationRequest);
    return objectMapper.writeValueAsString(submitApplicationRequest);
  }

  public static SubmitApplicationRequest generateSubmitApplicationRequest(
      ObjectMapper objectMapper, String requestTemplate) throws IOException {
    // Use json request file as a request template
    String submitApplicationRequestText =
        getResourceAsString(requestTemplate, StandardCharsets.UTF_8);

    // Set application files
    return objectMapper.readValue(submitApplicationRequestText, SubmitApplicationRequest.class);
  }

  private static ObjectMapper generateObjectMapper(boolean requestTemplateIsYaml) {
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

    return objectMapper;
  }

  public static void runSparkApplicationWithUnauthorizedAndAuthorizedUser(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier)
      throws Exception {
    boolean requestTemplateIsYaml = requestTemplate.toLowerCase().endsWith("yaml");
    ObjectMapper objectMapper = generateObjectMapper(requestTemplateIsYaml);
    SubmitApplicationRequest submitApplicationRequest =
        generateSubmitApplicationRequest(objectMapper, requestTemplate);
    String submitApplicationRequestText =
        generateSubmitApplicationRequestStr(
            objectMapper, submitApplicationRequest, requestModifier);
    String unauthorizedUser = "aimldpsecuritynoaccess_bot";
    String authorizedUser = "raimldpi";

    // Submit application with unauthorized user
    final String submitUrl = String.format("%s/spark", serviceRootUrl);
    String responseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            submitUrl,
            submitApplicationRequestText,
            unauthorizedUser,
            "post");
    assertResponseBasedOnAuthorizationStatus(responseStrWithUnauthorizedUser, false);

    // Submit application with authorized user
    String responseStrWithAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml, submitUrl, submitApplicationRequestText, authorizedUser, "post");
    assertResponseBasedOnAuthorizationStatus(responseStrWithAuthorizedUser, true);
    String submissionId =
        HttpUtils.parseJson(responseStrWithAuthorizedUser, SubmitApplicationResponse.class)
            .getSubmissionId();

    // Get submissions with unauthorized user
    String mySubmissionsResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/mySubmissions", serviceRootUrl),
            submitApplicationRequestText,
            unauthorizedUser,
            "get");
    Assert.assertTrue(mySubmissionsResponseStrWithUnauthorizedUser.equals("{\"submissions\":[]}"));

    // Get submissions with authorized user
    String mySubmissionsResponseStrWithAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/mySubmissions", serviceRootUrl),
            submitApplicationRequestText,
            authorizedUser,
            "get");
    Assert.assertTrue(!mySubmissionsResponseStrWithAuthorizedUser.equals("{\"submissions\":[]}"));

    // Get Spark application spec with unauthorized user
    String descResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/spec", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            unauthorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(descResponseStrWithUnauthorizedUser, false);

    // Get Spark application spec with authorized user
    String descResponseStrWithauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/spec", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            authorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(descResponseStrWithauthorizedUser, true);

    // Get Spark application status with unauthorized user
    String statusResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/status", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            unauthorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(statusResponseStrWithUnauthorizedUser, false);

    // Get Spark application status with authorized user
    String statusResponseStrWithAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/status", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            authorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(statusResponseStrWithAuthorizedUser, true);

    // Get Spark application driver information with unauthorized user
    String getDriverResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/driver", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            unauthorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(getDriverResponseStrWithUnauthorizedUser, false);

    // Get Spark application driver information with authorized user
    String getDriverResponseStrWitAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/driver", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            authorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(getDriverResponseStrWitAuthorizedUser, true);

    // Describe spark application with unauthorized user
    String describeResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/describe", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            unauthorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(describeResponseStrWithUnauthorizedUser, false);

    // Describe spark application with authorized user
    String describeResponseStrWithAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s/describe", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            authorizedUser,
            "get");
    assertResponseBasedOnAuthorizationStatus(describeResponseStrWithAuthorizedUser, true);

    // Delete application with unauthorized user
    String deleteResponseStrWithUnauthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            unauthorizedUser,
            "delete");
    assertResponseBasedOnAuthorizationStatus(deleteResponseStrWithUnauthorizedUser, false);

    // Delete application with authorized user
    String deleteResponseStrWithAuthorizedUser =
        submitRequest(
            requestTemplateIsYaml,
            String.format("%s/spark/%s", serviceRootUrl, submissionId),
            submitApplicationRequestText,
            authorizedUser,
            "delete");
    assertResponseBasedOnAuthorizationStatus(deleteResponseStrWithAuthorizedUser, true);
  }

  private static void assertResponseBasedOnAuthorizationStatus(
      String response, boolean authorized) {
    boolean unauthorizedResult =
        response.contains(String.valueOf(Response.Status.FORBIDDEN.getStatusCode()))
            && response.contains("Unauthorized");
    boolean result;
    if (authorized) {
      result = !unauthorizedResult;
    } else {
      result = unauthorizedResult;
    }
    Assert.assertTrue(result);
  }

  private static String getMediaType(boolean requestTemplateIsYaml) {
    if (requestTemplateIsYaml) {
      return "application/x-yaml";
    } else {
      return "application/json";
    }
  }

  private static String submitRequest(
      boolean requestTemplateIsYaml,
      String url,
      String submitApplicationRequestText,
      String user,
      String requestType)
      throws Exception {
    String mediaType = getMediaType(requestTemplateIsYaml);
    logger.info("Submitting application to {}", url);

    var builder = HttpRequest.newBuilder().uri(new URI(url)).header("Content-Type", mediaType);
    if (authHeaderName != null && !authHeaderName.isEmpty()) {
      builder = builder.header(authHeaderName, user);
    }
    HttpRequest request = null;
    if (requestType.equals("post")) {
      request =
          builder.POST(HttpRequest.BodyPublishers.ofString(submitApplicationRequestText)).build();
    } else if (requestType.equals("get")) {
      request = builder.GET().build();
    } else if (requestType.equals("delete")) {
      request = builder.DELETE().build();
    } else {
      throw new Exception(String.format("requestType %s is not supported.", requestType));
    }

    HttpResponse<String> response =
        HttpClient.newBuilder().build().send(request, HttpResponse.BodyHandlers.ofString());
    return response.body();
  }

  // When driverLog is true, the result type will be Some<String>, otherwise it would be None.
  private static Optional<String> runSparkApplication(
      String serviceRootUrl,
      String requestTemplate,
      Consumer<SubmitApplicationRequest> requestModifier,
      String expectedFinalState,
      boolean driverLog,
      boolean sparkSpec)
      throws IOException {
    boolean requestTemplateIsYaml = requestTemplate.toLowerCase().endsWith("yaml");
    ObjectMapper objectMapper = generateObjectMapper(requestTemplateIsYaml);
    SubmitApplicationRequest submitApplicationRequest =
        generateSubmitApplicationRequest(objectMapper, requestTemplate);
    String submitApplicationRequestText =
        generateSubmitApplicationRequestStr(
            objectMapper, submitApplicationRequest, requestModifier);

    // submit application
    final String submitUrl = String.format("%s/spark", serviceRootUrl);
    String mediaType = getMediaType(requestTemplateIsYaml);
    logger.info("Submitting application to {}", submitUrl);
    SubmitApplicationResponse submitApplicationResponse =
        post(
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

    while (System.currentTimeMillis() - startTime < 500000) {
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

    // fetch spark application spec
    String sparkSpecStr = getSparkSpecFromDescribe(serviceRootUrl, submissionId);

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
    } else if (sparkSpec) {
      return Optional.of(sparkSpecStr);
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

  private static String getSparkSpecFromDescribe(String serviceRootUrl, String submissionId)
      throws IOException {

    final String getDriverLogUrl =
        String.format("%s/spark/%s/describe", serviceRootUrl, submissionId);

    HttpResponse httpResponse =
        HttpUtils.getHttpResponse(getDriverLogUrl, authHeaderName, authHeaderValue);
    logger.info("Reading Spark Spec for submission id: {}", submissionId);

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
