package com.apple.spark.appleintegration;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
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
import org.testng.annotations.Test;

/*
Need to set following environment variables to run tests in this class:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
The values could be get by running awsappleconnect command, for example:
  awsappleconnect -u bo_yang6 -a 606462133786 -r admin
 */
public class S3RestIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(S3RestIntegrationTest.class);
  private final String authHeaderName = "Authorization";
  private final String authHeaderValue = "Basic dXNlcjE6cGFzc3dvcmQ=";
  private final SkateTestSupport testSupport = new SkateTestSupport();
  private String serviceRootUrl;

  @BeforeClass
  public void beforeClass() throws Exception {
    testSupport.before();
    serviceRootUrl = String.format("http://localhost:%s/skatev1", testSupport.getLocalPort());
  }

  @AfterClass
  public void afterClass() {
    testSupport.after();
  }

  @Test
  public void uploadFileWithoutFolder() throws IOException {
    URL resourceUrl =
        this.getClass().getResource("/SubmitSparkApplicationRequest_python_example.json");
    byte[] resourceBytes = Resources.toByteArray(resourceUrl);

    String s3ObjectKeyName = "s3-upload-integration-test-file.binary";
    final String uploadUrl = String.format("%s/s3/%s", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);
  }

  @Test
  public void uploadFileWithFolder() throws IOException {
    URL resourceUrl =
        this.getClass().getResource("/SubmitSparkApplicationRequest_python_example.json");
    byte[] resourceBytes = Resources.toByteArray(resourceUrl);

    String s3ObjectKeyName = "s3-upload-integration-test-file.binary";
    String uploadUrl = String.format("%s/s3/%s?folder=", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);

    uploadUrl = String.format("%s/s3/%s?folder=folder1", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);

    uploadUrl = String.format("%s/s3/%s?folder=folder1/folder2", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);

    uploadUrl = String.format("%s/s3/%s?folder=/folder1/folder2", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);

    uploadUrl = String.format("%s/s3/%s?folder=/folder1/folder2/", serviceRootUrl, s3ObjectKeyName);
    uploadFileAndVerify(uploadUrl, resourceBytes);

    uploadUrl =
        String.format(
            "%s/s3/%s?folder=%%2Ffolder1%%2Ffolder2/folder3", serviceRootUrl, s3ObjectKeyName);
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

    Regions clientRegion = Regions.US_WEST_2;

    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
    S3Object s3Object = s3Client.getObject(bucket, objectKey);
    Assert.assertEquals(s3Object.getObjectMetadata().getContentLength(), size);

    byte[] s3ObjectBytes = IOUtils.toByteArray(s3Object.getObjectContent());
    Assert.assertEquals(s3ObjectBytes.length, size);
    Assert.assertEquals(s3ObjectBytes, resourceBytes);
  }
}
