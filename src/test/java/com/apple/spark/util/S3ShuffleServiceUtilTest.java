package com.apple.spark.util;

import static com.apple.spark.util.S3ShuffleServiceUtil.randomSelectShuffleBucketByWeight;

import com.apple.spark.AppConfig;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class S3ShuffleServiceUtilTest {

  @Test
  public void testRandomSelectShuffleBucketByWeight() {
    List<AppConfig.ShuffleS3Bucket> shuffleS3Buckets = new ArrayList<>();
    shuffleS3Buckets.add(new AppConfig.ShuffleS3Bucket("bucket_1", 2.0));
    shuffleS3Buckets.add(new AppConfig.ShuffleS3Bucket("bucket_2", 3.0));
    shuffleS3Buckets.add(new AppConfig.ShuffleS3Bucket("bucket_3", 5.0));
    int bucket_1_count = 0;
    int bucket_2_count = 0;
    int bucket_3_count = 0;
    // run randomSelectShuffleBucketByWeight 1000 times
    // and record the selection counts for each bucket
    for (int i = 0; i < 1000; i++) {
      switch (randomSelectShuffleBucketByWeight(shuffleS3Buckets)) {
        case "bucket_1":
          bucket_1_count++;
          break;
        case "bucket_2":
          bucket_2_count++;
          break;
        case "bucket_3":
          bucket_3_count++;
          break;
      }
    }
    // assert the selected bucket counts reflect their weights
    Assert.assertTrue(bucket_1_count > 50 && bucket_1_count < 400);
    Assert.assertTrue(bucket_2_count > 100 && bucket_1_count < 500);
    Assert.assertTrue(bucket_3_count > 200 && bucket_1_count < 800);
  }
}
