package com.apple.spark.api;

import static com.apple.spark.core.Constants.*;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SubmissionSummaryTest {
  @Test
  public void testEnvToDashboardTransformation() {
    SubmissionSummary submission = new SubmissionSummary();
    String url = submission.getDashboardUrl(SIRI_APP_METRICS_DASHBOARD);
    Assert.assertTrue(url.contains(SIRI_APP_METRICS_DASHBOARD));
    // Contains drop-down setting parameter
    Assert.assertTrue(url.contains("?tpl_var_submission_id="));
  }

  @Test
  public void testEnvToSplunkTransformation() {
    SubmissionSummary submission = new SubmissionSummary();
    String url = submission.getSplunkUrl(SIRI_SPLUNK);
    Assert.assertTrue(url.contains(SIRI_SPLUNK));
    // Contains pod name filter
    Assert.assertTrue(url.contains("kubernetes.pod_name"));
  }
}
