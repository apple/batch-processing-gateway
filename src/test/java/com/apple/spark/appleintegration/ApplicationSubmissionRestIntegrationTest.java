package com.apple.spark.appleintegration;

import static com.apple.spark.core.Constants.DEFAULT_DB_NAME;

import com.apple.spark.core.Constants;
import com.apple.spark.core.DBConnection;
import com.apple.spark.core.QueueTokenVerifier;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.util.JwtUtils;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/*
This integration test starts a Skate server and send HTTP request to run Spark application.
It will use config.yml in root of this project to configure the Skate server. You could change
config.yml in your local side to use different environments.
 */
public class ApplicationSubmissionRestIntegrationTest {

  private static final String authHeaderName = "Authorization";
  private static final String authHeaderValue = "Basic dXNlcjE6cGFzc3dvcmQ=";
  private static final String skateIntegrationTestResourcesFolderUrl =
      "s3a://aiml-prod-artifacts/skate_uploaded/skateIntegrationTest/resources";

  private final SkateTestSupport testSupport = new SkateTestSupport();

  private String serviceRootUrl;

  @DataProvider(name = "sparkVersions")
  public static Object[][] sparkVersions() {
    return new Object[][] {{"3.2"}};
  }

  @DataProvider(name = "sparkVersionsAndPythonApplications")
  public static Object[][] sparkVersionsAndPythonApplications() {
    return new Object[][] {
      {"3.2", "/SparkHiveExampleApp.py"},
      {"3.2", "/SparkExampleApp.py"}
    };
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    testSupport.before();
    serviceRootUrl =
        String.format(
            "http://localhost:%s%s",
            testSupport.getLocalPort(), Constants.DEFAULT_APPLICATION_CONTEXT_PATH);
  }

  @AfterClass
  public void afterClass() {
    testSupport.after();
  }

  @Test(dataProvider = "sparkVersions")
  // Use previously uploaded jar file to run Spark application
  public void runJavaSparkApplicationWithPreUploadedFile(String sparkVersion) throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_java_example.json";
    final String sparkApplicationFile =
        String.format(
            "s3a://%s/testApplications/spark-java-examples.jar",
            testSupport.getConfiguration().getS3Bucket());

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setQueue("poc");
        });
  }

  @Test(dataProvider = "sparkVersionsAndPythonApplications")
  // Upload python file and run Spark application
  public void runPythonSparkApplicationWithUpload(String sparkVersion, String sparkApplication)
      throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String mainClass = null;
    final String dependencyPyFile = null;

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setQueue("poc");
        });
  }

  @Test
  // Upload python file and run Spark application, then verify application is logged in DB
  public void runPythonSparkApplicationWithUpload_verifyDB() throws Exception {
    final String sparkVersion = "3.2";
    final String sparkApplication = "/SparkExampleApp.py";
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String applicationName = " skate: integration test " + UUID.randomUUID();
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setApplicationName(applicationName);
        });

    // Check application status in DB
    boolean foundApplicationStatusInDB = false;
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 180000) {
      DBConnection dbConnection =
          new DBConnection(
              testSupport.getConfiguration().getDbStorageSOPS().getConnectionString(), "sa", "sa");
      String sql =
          String.format(
              "SELECT * from %s.application_submission where request_body like '%%%s%%' limit 1",
              DEFAULT_DB_NAME, applicationName);
      try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          Assert.assertTrue(resultSet.next());
          String requestBody = resultSet.getString("request_body");
          Assert.assertTrue(requestBody.contains(applicationName));
          String status = resultSet.getString("status");
          if (SparkConstants.COMPLETED_STATE.equals(status)) {
            foundApplicationStatusInDB = true;
            break;
          }
        }
      }
      dbConnection.close();
    }
    Assert.assertTrue(foundApplicationStatusInDB);
  }

  @Test
  public void runPythonSparkApplicationWithFailedStatus() throws IOException {
    final String sparkVersion = "3.2";
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplication = "/SparkExampleAppWhichWillFail.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    IntegrationTestHelper.runSparkApplicationWhichWillFail(
        serviceRootUrl, requestTemplate, sparkVersion, sparkApplicationFile);
  }

  @Test(dataProvider = "sparkVersions")
  // Upload python file and run Spark application with YAML request format
  public void runPythonSparkApplicationWithYamlRequest(String sparkVersion) throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.yaml";
    final String sparkApplication = "/SparkExampleApp.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String mainClass = null;
    final String dependencyPyFile = null;
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile);
  }

  @Test(dataProvider = "sparkVersions")
  // run Spark application with custom image specified in YAML request format
  public void runPythonSparkApplicationWithYamlRequestCustomImage(String sparkVersion)
      throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_custom_image.yaml";
    final String sparkApplication = "/SparkExampleApp.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String mainClass = null;
    final String dependencyPyFile = null;
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile);
  }

  @Test(dataProvider = "sparkVersions")
  // Upload python application file and dependency library file, then run Spark application
  public void runPythonSparkApplicationWithDependencyFile(String sparkVersion) throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplication = "/SparkExampleAppWithPythonDependency.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String mainClass = null;
    final String dependencyPyFile = skateIntegrationTestResourcesFolderUrl + "/pythonLibExample.py";
    final String dependencyFile =
        skateIntegrationTestResourcesFolderUrl
            + "/SubmitSparkApplicationRequest_python_example.json";
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        dependencyFile);
  }

  @Test
  public void runPythonSparkApplicationWithSecureQueue() throws IOException {
    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";

    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    String tokenSecret = testSupport.getConfiguration().getQueueTokenSOPS().getSecrets().get(0);
    String token =
        JwtUtils.createToken(
            tokenSecret,
            QueueTokenVerifier.ISSUER_ADMIN,
            "unitTestQueueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"canary02_secure"});
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setQueue("canary02_secure");
          submitApplicationRequest.setQueueToken(token);
        });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void runPythonSparkApplicationWithSecureQueue_noQueueToken() throws IOException {
    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setQueue("canary_secure");
        });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void runPythonSparkApplicationWithSecureQueue_invalidQueueTokenSecret()
      throws IOException {
    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    String token =
        JwtUtils.createToken(
            "invalid_secret",
            QueueTokenVerifier.ISSUER_ADMIN,
            "unitTestQueueToken",
            QueueTokenVerifier.CLAIM_ALLOWED_QUEUES,
            new String[] {"canary_secure"});
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setQueue("canary_secure");
          submitApplicationRequest.setQueueToken(token);
        });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void runPythonSparkApplicationWithTooLargeMaxRunningMillis() throws IOException {
    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkConf(new HashMap<>());
          submitApplicationRequest.getSparkConf().put("skate.maxRunningMillis", "43200001");
        });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void runJavaSparkApplicationWithTooManyExecutorsInSparkConfig() throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_java_example.json";
    final String sparkVersion = "3.2";
    final String sparkApplicationFile =
        String.format(
            "s3a://%s/testApplications/spark-java-examples.jar",
            testSupport.getConfiguration().getS3Bucket());
    final String mainClass = "com.apple.spark.examples.OneStageApp";
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setMainClass(mainClass);
          Map<String, String> sparkConf = new HashMap<>();
          sparkConf.put("spark.executor.instances", "90000");
          submitApplicationRequest.setSparkConf(sparkConf);
        });
  }

  @Test
  // Ensuring app-level logs exist. Its missing can be due to: e.g. log4j upgrades and *new version*
  // of log4j properties file not correctly configured
  // See rdar://95687978 for more details.
  public void runJavaSparkApplicationWithApplicationLevelLog() throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_java_example.json";
    final String sparkVersion = "3.2";
    // This jar is built from spark-examples module, with only SparkPi.scala being modified: 4 lines
    // of logging are
    // added, levels from Debug to Error. Refer to test/resources/SparkPi_modified.scala for the
    // codes
    final String sparkApplicationFile =
        String.format(
            "s3a://%s/skate_uploaded/test/app-level-logs-verification-spark-examples_2.12-3.2.0.26-apple-aiml-SNAPSHOT.jar",
            testSupport.getConfiguration().getS3Bucket());

    final String mainClass = "org.apache.spark.examples.SparkPi";

    String driverLogs =
        IntegrationTestHelper.runSparkAppAndGetDriverLog(
            serviceRootUrl,
            requestTemplate,
            submitApplicationRequest -> {
              submitApplicationRequest.setSparkVersion(sparkVersion);
              submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
              submitApplicationRequest.setMainClass(mainClass);
            });

    Assert.assertTrue(driverLogs.contains("This is an application-level INFO log"));
    Assert.assertTrue(driverLogs.contains("This is an application-level WARN log"));
    Assert.assertTrue(driverLogs.contains("This is an application-level ERROR log"));
  }
}
