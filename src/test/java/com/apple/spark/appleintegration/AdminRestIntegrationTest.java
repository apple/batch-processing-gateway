package com.apple.spark.appleintegration;

import static com.apple.spark.appleintegration.ApplicationSubmissionRestIntegrationTest.skateIntegrationTestResourcesFolderUrl;
import static com.apple.spark.core.BatchSchedulerConstants.YUNIKORN_ROOT_QUEUE;

import com.apple.spark.api.GetJobsResponse;
import com.apple.spark.api.SubmissionSummary;
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

  private String GATEWAY_CI_WITH_PREFIX = YUNIKORN_ROOT_QUEUE + "." + "gateway-ci";

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
  public void testQueues() {
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";
    String fullUrl = serviceRootUrl + "/admin/queues";
    String response = HttpUtils.get(fullUrl, authHeaderName, authHeaderValue);

    Assert.assertTrue(response.contains(GATEWAY_CI_WITH_PREFIX));
  }

  @Test
  public void testJobs() throws IOException {

    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";

    // Preparation: Submit a job
    String submissionId =
        IntegrationTestHelper.runSparkAppAndGetSubmissionId(
            serviceRootUrl,
            requestTemplate,
            submitApplicationRequest -> {
              submitApplicationRequest.setMainApplicationFile(sparkApplicationFile);
              submitApplicationRequest.setQueue("gateway-ci");
            });

    String filterUrl =
        serviceRootUrl + "/admin/jobs?queue=root.gateway-ci&submission_id=" + submissionId;
    String responseFilteredStr = HttpUtils.get(filterUrl, authHeaderName, authHeaderValue);
    GetJobsResponse responseFiltered =
        HttpUtils.parseJson(responseFilteredStr, GetJobsResponse.class);

    Assert.assertEquals((int) responseFiltered.get_page_info().get("num"), 1);
  }

  @Test
  public void testJobsWithQueueParam() throws IOException {

    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";

    String fullUrl = serviceRootUrl + "/admin/jobs?queue=root.gateway-ci";

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

    for (SubmissionSummary job : response.getJobs()) {
      Assert.assertEquals(job.getQueue(), GATEWAY_CI_WITH_PREFIX);
    }
  }

  @Test
  public void testJobsWithQueueAndUserParam() throws IOException {

    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";

    String fullUrl = serviceRootUrl + "/admin/jobs?queue=root.gateway-ci&user=chenzhao_guo";

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

    for (SubmissionSummary job : response.getJobs()) {
      Assert.assertEquals(job.getQueue(), GATEWAY_CI_WITH_PREFIX);
      Assert.assertEquals(job.getUser(), "chenzhao_guo");
    }
  }

  @Test
  public void testJobsWithSortByParam() throws IOException {

    String requestTemplate = "/SubmitSparkApplicationRequest_python_example.json";
    String sparkApplication = "/SparkExampleApp.py";
    String sparkApplicationFile = skateIntegrationTestResourcesFolderUrl + sparkApplication;
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";

    String fullUrl = serviceRootUrl + "/admin/jobs?sort=duration";

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

    // Default order is desc
    Long lastDuration = Long.MAX_VALUE;
    for (SubmissionSummary job : response.getJobs()) {
      Assert.assertTrue(job.getDuration() < lastDuration);
      lastDuration = job.getDuration();
    }
  }
}
