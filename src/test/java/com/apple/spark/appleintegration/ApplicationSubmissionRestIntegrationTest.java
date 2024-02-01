package com.apple.spark.appleintegration;

import static com.apple.spark.core.Constants.DEFAULT_DB_NAME;

import com.apple.spark.core.Constants;
import com.apple.spark.core.DBConnection;
import com.apple.spark.core.SparkConstants;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
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
  protected static final String skateIntegrationTestResourcesFolderUrl =
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

  @Test(dataProvider = "sparkVersionsAndPythonApplications")
  // Upload python file and run Spark application
  public void runPythonSparkApplicationWithUnauthorizedUser(
      String sparkVersion, String sparkApplication) throws Exception {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;

    IntegrationTestHelper.runSparkApplicationWithUnauthorizedAndAuthorizedUser(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setQueue("queue-authz-poc");
        });
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
          submitApplicationRequest.setQueue("gateway-ci");
        });
  }

  @Test(dataProvider = "sparkVersionsAndPythonApplications")
  // Upload python file and run Spark application
  public void runPythonSparkApplicationWithUpload(String sparkVersion, String sparkApplication)
      throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkVersion(sparkVersion);
          submitApplicationRequest.setQueue("gateway-ci");
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

    final ArrayList<List<String>> argumentsList = new ArrayList<>();
    // Check either case, with and without arguments
    argumentsList.add(List.of("2", "3", "1"));
    argumentsList.add(List.of());

    for (List<String> args : argumentsList) {

      IntegrationTestHelper.runSparkApplication(
          serviceRootUrl,
          requestTemplate,
          submitApplicationRequest -> {
            submitApplicationRequest.setSparkVersion(sparkVersion);
            submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
            submitApplicationRequest.setApplicationName(applicationName);
            submitApplicationRequest.setQueue("gateway-ci");
            if (args.size() != 0) {
              submitApplicationRequest.setArguments(args);
            }
          });

      // Check application status in DB
      boolean foundApplicationStatusInDB = false;
      long startTime = System.currentTimeMillis();
      /* Below test are failing with Table "APPLICATION_SUBMISSION" not found (this database is empty)..Need to be investigated */
      while (System.currentTimeMillis() - startTime < 180000) {
        DBConnection dbConnection =
            new DBConnection(
                testSupport.getConfiguration().getDbStorageSOPS().getConnectionString(),
                "sa",
                "sa");
        String sql =
            String.format(
                "SELECT * from %s.application_submission where request_body like '%%%s%%' limit 1",
                DEFAULT_DB_NAME, applicationName);
        try (PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql)) {
          try (ResultSet resultSet = statement.executeQuery()) {
            Assert.assertTrue(resultSet.next());
            String requestBody = resultSet.getString("request_body");
            String arguments = resultSet.getString("arguments");

            Assert.assertTrue(requestBody.contains(applicationName));

            String argumentsFromRequestBody;
            // It's possible that request_body field doesn't contain arguments field
            if (requestBody.contains("arguments\":[")) {
              String[] split = requestBody.split("arguments\":\\[")[1].split("]")[0].split(",");
              List<String> trimmed =
                  Arrays.stream(split)
                      .map(s -> s.trim().replace("\"", ""))
                      .collect(Collectors.toList());
              argumentsFromRequestBody = String.join(" ", trimmed);
            } else {
              argumentsFromRequestBody = "";
            }

            Assert.assertTrue(arguments.equals(argumentsFromRequestBody));

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
  }

  @Test
  public void runPythonSparkApplicationWithFailedStatus() throws IOException {
    final String sparkVersion = "3.2";
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplication = "/SparkExampleAppWhichWillFail.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String queue = "gateway-ci";
    IntegrationTestHelper.runSparkApplicationWhichWillFail(
        serviceRootUrl, requestTemplate, sparkVersion, sparkApplicationFile, queue);
  }

  @Test(dataProvider = "sparkVersions")
  // Upload python file and run Spark application with YAML request format
  public void runPythonSparkApplicationWithYamlRequest(String sparkVersion) throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.yaml";
    final String sparkApplication = "/SparkExampleApp.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String mainClass = null;
    final String dependencyPyFile = null;
    final String queue = "gateway-ci";
    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        queue);
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
    final String queue = "gateway-ci";

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        queue);
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
    final String queue = "gateway-ci";

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        sparkVersion,
        sparkApplicationFile,
        mainClass,
        dependencyPyFile,
        dependencyFile,
        queue);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void runPythonSparkApplicationWithTooLargeMaxRunningMillis() throws IOException {
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    final String sparkApplication = "/SparkExampleApp.py";
    final String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String queue = "gateway-ci";

    IntegrationTestHelper.runSparkApplication(
        serviceRootUrl,
        requestTemplate,
        submitApplicationRequest -> {
          submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
          submitApplicationRequest.setSparkConf(new HashMap<>());
          submitApplicationRequest.getSparkConf().put("skate.maxRunningMillis", "43200001");
          submitApplicationRequest.setQueue(queue);
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
    final String queue = "gateway-ci";

    String driverLogs =
        IntegrationTestHelper.runSparkAppAndGetDriverLog(
            serviceRootUrl,
            requestTemplate,
            submitApplicationRequest -> {
              submitApplicationRequest.setSparkVersion(sparkVersion);
              submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
              submitApplicationRequest.setMainClass(mainClass);
              submitApplicationRequest.setQueue(queue);
            });

    Assert.assertTrue(driverLogs.contains("This is an application-level INFO log"));
    Assert.assertTrue(driverLogs.contains("This is an application-level WARN log"));
    Assert.assertTrue(driverLogs.contains("This is an application-level ERROR log"));
  }
}
