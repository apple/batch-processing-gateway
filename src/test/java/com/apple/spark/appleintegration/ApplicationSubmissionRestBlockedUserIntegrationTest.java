package com.apple.spark.appleintegration;

import com.apple.spark.core.Constants;
import java.io.IOException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
This integration test starts a Skate server and send HTTP request to run Spark application.
It will use config.yml in root of this project to configure the Skate server. You could change
config.yml in your local side to use different environments.
 */
public class ApplicationSubmissionRestBlockedUserIntegrationTest {

  private static final String authHeaderName = "Authorization";
  private static final String authHeaderValue =
      "Basic bYmxvY2tlZF91c2VyXzE6cGFzc3dvcmQ="; // blocked_user_1:password
  final String skateIntegrationTestResourcesFolderUrl =
      "s3a://aiml-prod-artifacts/skate_uploaded/skateIntegrationTest/resources";

  private final SkateTestSupport testSupport = new SkateTestSupport();

  private String serviceRootUrl;

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

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to execute POST.*")
  // Upload python file and run Spark application
  public void runPythonSparkApplicationWithUpload() throws IOException {
    final String sparkApplication = "/SparkExampleApp.py";
    final String sparkVersion = "2.4";
    final String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
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
}
