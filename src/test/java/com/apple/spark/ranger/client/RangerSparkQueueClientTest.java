package com.apple.spark.ranger.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RangerSparkQueueClientTest {

  RangerSparkQueueClient rangerSparkQueueClient =
      new RangerSparkQueueClient(
          "https://ranger-1-siri-test.aws.ocean.g.apple.com",
          "https://solr-1-siri-test.aws.ocean.g.apple.com/solr/ranger_audits");
  String queue = "poc_02";
  String authorizedUser = "raimldpi";
  String unauthorizedUser = "user";

  @Test
  public void authorizeUserOnStatusList() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "status", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }

  @Test
  public void doNotAuthorizeUserOnStatusList() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "status", unauthorizedUser));
    Assert.assertEquals(authorizeRes, false);
  }

  @Test
  public void authorizeUserOnOperationList() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(new QueueAccessTypeAndUser(queue, "list", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }

  @Test
  public void doNotAuthorizeUserOnOperationList() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "list", unauthorizedUser));
    Assert.assertEquals(authorizeRes, false);
  }

  @Test
  public void authorizeUserOnOperation() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "submit", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }

  //    @Test
  //    public void doNotAuthorizeUserOnOperationLog() throws Exception {
  //      boolean authorizeRes =
  //          rangerSparkQueueClient.authorize(
  //              new QueueAccessTypeAndUser(queue, "log", unauthorizedUser));
  //      Assert.assertEquals(authorizeRes, false);
  //    }
  //
  //    @Test
  //    public void authorizeUserOnOperationLog() throws Exception {
  //      boolean authorizeRes =
  //          rangerSparkQueueClient.authorize(new QueueAccessTypeAndUser(queue, "log",
  //   authorizedUser));
  //      Assert.assertEquals(authorizeRes, true);
  //    }

  @Test
  public void doNotAuthorizeUserOnOperationKill() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "kill", unauthorizedUser));
    Assert.assertEquals(authorizeRes, false);
  }

  @Test
  public void authorizeUserOnOperationKill() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(new QueueAccessTypeAndUser(queue, "kill", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }

  @Test
  public void doNotAuthorizeUserOnOperationSubmit() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "submit", unauthorizedUser));
    Assert.assertEquals(authorizeRes, false);
  }

  @Test
  public void authorizeUserOnOperationSubmit() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "submit", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }

  @Test
  public void doNotAuthorizeUserOnOperationDescribe() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "describe", unauthorizedUser));
    Assert.assertEquals(authorizeRes, false);
  }

  @Test
  public void authorizeUserOnOperationDescribe() throws Exception {
    boolean authorizeRes =
        rangerSparkQueueClient.authorize(
            new QueueAccessTypeAndUser(queue, "describe", authorizedUser));
    Assert.assertEquals(authorizeRes, true);
  }
}
