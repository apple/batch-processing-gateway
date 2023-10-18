package com.apple.spark.appleintegration;

import com.apple.spark.core.Constants;
import com.apple.spark.rest.AdminRest;
import com.apple.spark.util.HttpUtils;
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
  public void test() {
    final String authHeaderName = "X-Appleconnect-Acaccountname";
    final String authHeaderValue = "raimldpi";
    String fullUrl = serviceRootUrl + "/admin/statuses";
    String response = HttpUtils.get(fullUrl, authHeaderName, authHeaderValue);

    for (String status : AdminRest.allPossibleStatuses) {
      Assert.assertTrue(response.contains(status));
    }
  }
}
