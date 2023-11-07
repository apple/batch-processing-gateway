package com.apple.spark.ranger.client;

import com.apple.spark.AppConfig;
import com.apple.spark.appleintegration.SkateTestSupport;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RangerSparkQueueClientTest {

  private final SkateTestSupport testSupport = new SkateTestSupport();
  private RangerSparkQueueClient rangerSparkQueueClient;
  String queue = "poc_02";
  String authorizedUser = "raimldpi";
  String unauthorizedUser = "user";

  @BeforeClass
  public void BeforeClass() throws Exception {
    testSupport.before();
    AppConfig.Ranger rangerConfig = testSupport.getConfiguration().getRanger();
    this.rangerSparkQueueClient =
        new RangerSparkQueueClient(
            rangerConfig.getSparkQueuePolicyRestUrl(),
            rangerConfig.getSparkQueueXasecureAuditDestinationSolrUrls(),
            rangerConfig.getUser(),
            rangerConfig.getPasswordDecoded());
  }

  @AfterClass
  public void AfterClass() {
    testSupport.after();
  }

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
