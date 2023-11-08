package com.apple.spark.appleintegration;

import static com.apple.spark.appleintegration.ApplicationSubmissionRestIntegrationTest.skateIntegrationTestResourcesFolderUrl;

import com.apple.spark.api.GetJobsResponse;
import com.apple.spark.core.Constants;
import com.apple.spark.util.HttpUtils;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdminRestIntegrationTest {

  private final SkateTestSupport testSupport = new SkateTestSupport();

  private String serviceRootUrl;

  @BeforeMethod
  public void setUp() throws Exception {
    testSupport.before();
    serviceRootUrl =
        String.format(
            "http://localhost:%s%s",
            testSupport.getLocalPort(), Constants.DEFAULT_APPLICATION_CONTEXT_PATH);
  }

  @AfterMethod
  public void tearDown() {
    testSupport.after();
  }

  @Test
  public void testStatuses() {
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";
    String fullUrl = serviceRootUrl + "/admin/statuses";
    String response = HttpUtils.get(fullUrl, authHeaderName, authHeaderValue);

    for (String status : Constants.ALL_POSSIBLE_STATUSES) {
      Assert.assertTrue(response.contains(status));
    }
  }

  @Test
  public void testJobs() throws IOException {

    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";

    String fullUrl = serviceRootUrl + "/admin/jobs?queue=root.gateway-ci";

    String responseBeforeSubmissionStr = HttpUtils.get(fullUrl, authHeaderName, authHeaderValue);
    GetJobsResponse responseBeforeSubmission =
        HttpUtils.parseJson(responseBeforeSubmissionStr, GetJobsResponse.class);
    int numJobsBeforeSubmission = responseBeforeSubmission.get_page_info().get("total");

    // Preparation: Submit a job
    String submissionId =
        IntegrationTestHelper.runSparkAppAndGetSubmissionId(
            serviceRootUrl,
            requestTemplate,
            submitApplicationRequest -> {
              submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
              submitApplicationRequest.setQueue("gateway-ci");
            });

    String responseStr = HttpUtils.get(fullUrl, authHeaderName, authHeaderValue);
    GetJobsResponse response = HttpUtils.parseJson(responseStr, GetJobsResponse.class);

    Assert.assertEquals((int) response.get_page_info().get("total"), numJobsBeforeSubmission + 1);

    String filterUrl =
        serviceRootUrl + "/admin/jobs?queue=root.gateway-ci&submission_id=" + submissionId;
    String responseFilteredStr = HttpUtils.get(filterUrl, authHeaderName, authHeaderValue);
    GetJobsResponse responseFiltered =
        HttpUtils.parseJson(responseFilteredStr, GetJobsResponse.class);

    Assert.assertEquals((int) responseFiltered.get_page_info().get("total"), 1);
  }
}
