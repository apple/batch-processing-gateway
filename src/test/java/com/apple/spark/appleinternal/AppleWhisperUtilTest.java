package com.apple.spark.appleinternal;

import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AppleWhisperUtilTest {
  @Test
  public void enableWhisperSupportTest() {
    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    String proxyUser = "dummyUser";
    Map<String, String> annotations = new HashMap<>();
    annotations.put(WhisperConstants.WHISPER_NAMESPACE_KEY, "dummyNamespace");
    annotations.put(WhisperConstants.WHISPER_BUCKET_KEY_1, "MySecretBucket");
    annotations.put(WhisperConstants.WHISPER_IDENTITY_TYPE_KEY, "hydrogen");
    DriverSpec driverSpec = new DriverSpec();
    driverSpec.setAnnotations(annotations);
    sparkSpec.setDriver(driverSpec);

    AppleWhisperUtil.enableWhisperSupport(sparkSpec, proxyUser);

    Assert.assertEquals(
        sparkSpec.getDriver().getAnnotations().get(WhisperConstants.WHISPER_HYDROGEN_USER_KEY),
        "dummyUser");
    Assert.assertEquals(
        sparkSpec.getDriver().getAnnotations().get(WhisperConstants.WHISPER_ISSUERNAME_KEY),
        WhisperConstants.WHISPER_ISSUERNAME);
  }

  @Test
  public void nonAnnotationsTest() {
    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    String proxyUser = "dummyUser";
    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);
    try {
      AppleWhisperUtil.enableWhisperSupport(sparkSpec, proxyUser);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "SparkSpec doesn't have any annotations!");
    }
  }

  @Test
  public void withoutBucketTest() {
    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    String proxyUser = "dummyUser";
    Map<String, String> annotations = new HashMap<>();
    annotations.put(WhisperConstants.WHISPER_NAMESPACE_KEY, "dummyNamespace");
    DriverSpec driverSpec = new DriverSpec();
    driverSpec.setAnnotations(annotations);
    sparkSpec.setDriver(driverSpec);

    AppleWhisperUtil.enableWhisperSupport(sparkSpec, proxyUser);

    Assert.assertFalse(
        sparkSpec
            .getDriver()
            .getAnnotations()
            .containsKey(WhisperConstants.WHISPER_HYDROGEN_USER_KEY));
    Assert.assertFalse(
        sparkSpec
            .getDriver()
            .getAnnotations()
            .containsKey(WhisperConstants.WHISPER_ISSUERNAME_KEY));
  }
}
