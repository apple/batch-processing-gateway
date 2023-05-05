package com.apple.spark.appleintegration;

import com.apple.spark.api.UploadS3Response;
import com.apple.spark.util.HttpUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.util.Resources;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/*
Need to set environment variable like following to run tests in this class:
  GOOGLE_APPLICATION_CREDENTIALS=/Users/bo_yang6/.config/gcloud/legacy_credentials/bo_yang6@apple.com/adc.json
 */
public class S3RestGcsIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(S3RestGcsIntegrationTest.class);
  private final String authHeaderName = "Authorization";
  private final String authHeaderValue = "Basic dXNlcjE6cGFzc3dvcmQ=";
  private final SkateTestSupport testSupport = new SkateTestSupport();
  private String serviceRootUrl;

  @BeforeClass
  public void beforeClass() throws Exception {
    testSupport.before();
    testSupport
        .getConfiguration()
        .setS3Bucket("gs://spark-gcs-01-machinery2-prod-1xwp-8dfp8-7rkx6");
    serviceRootUrl = String.format("http://localhost:%s/skatev2", testSupport.getLocalPort());
  }

  @AfterClass
  public void afterClass() {
    testSupport.after();
  }

  // @Test // disable test by default, since it needs GCP credential to run
  public void uploadFileWithoutFolder() throws IOException {
    URL resourceUrl =
        this.getClass().getResource("/SubmitSparkApplicationRequest_python_example.json");
    byte[] resourceBytes = Resources.toByteArray(resourceUrl);

    String s3ObjectKeyName = "s3-upload-integration-test-file.binary";
    final String uploadUrl = String.format("%s/s3/%s", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);
  }

  private void uploadFileAndVerify(String s3Url, byte[] resourceBytes) throws IOException {
    int size = resourceBytes.length;
    UploadS3Response uploadS3Response =
        HttpUtils.post(
            s3Url,
            new ByteArrayInputStream(resourceBytes),
            authHeaderName,
            authHeaderValue,
            size,
            UploadS3Response.class);
    logger.info("Got response: {}", new ObjectMapper().writeValueAsString(uploadS3Response));
    String url = uploadS3Response.getUrl();
    URI uri = URI.create(url);
    String bucket = uri.getHost();
    String objectKey = StringUtils.removeStart(uri.getPath(), "/");
    Assert.assertNotNull(bucket);
    Assert.assertNotNull(objectKey);
  }
}
