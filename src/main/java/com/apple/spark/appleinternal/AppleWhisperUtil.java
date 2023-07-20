package com.apple.spark.appleinternal;

import com.apple.spark.operator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppleWhisperUtil {
  private static final Logger logger = LoggerFactory.getLogger(AppleWhisperUtil.class);

  public static void enableWhisperSupport(SparkApplicationSpec sparkSpec, String proxyUser) {
    try {
      // If request Whisper secrets, add following driver annotations as required by
      // whisper-admission-controller
      if (sparkSpec.getDriver().getAnnotations().containsKey(WhisperConstants.WHISPER_BUCKET_KEY_1)
          || sparkSpec
              .getDriver()
              .getAnnotations()
              .containsKey(WhisperConstants.WHISPER_BUCKET_KEY_2)) {
        sparkSpec
            .getDriver()
            .getAnnotations()
            .put(WhisperConstants.WHISPER_HYDROGEN_USER_KEY, proxyUser);
        sparkSpec
            .getDriver()
            .getAnnotations()
            .put(WhisperConstants.WHISPER_ISSUERNAME_KEY, WhisperConstants.WHISPER_ISSUERNAME);
      }
    } catch (Exception e) {
      logger.warn("SparkSpec doesn't have any annotations!");
    }
  }
}
