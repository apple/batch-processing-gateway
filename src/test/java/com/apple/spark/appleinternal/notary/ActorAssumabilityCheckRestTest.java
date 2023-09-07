package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.security.User;
import com.codahale.metrics.SharedMetricRegistries;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

public class ActorAssumabilityCheckRestTest {
  private static final String registryName = "test-registry";
  private static AppConfig appconfi;
  private static SimpleMeterRegistry meterRegistry;
  private static ActorAssumabilityCheckRest actorAssumabilityCheckRest;
  private static List<VirtualSparkClusterSpec> sparkClusters;

  @BeforeClass
  public static void setupClass() {
    SharedMetricRegistries.setDefault(registryName);
    meterRegistry = new SimpleMeterRegistry();
    appconfi = new AppConfig();
    AppConfig.NotaryAppConfig notary = new AppConfig.NotaryAppConfig();
    notary.setAacRateLimit(1000);
    appconfi.setNotary(notary);
    AppConfig.DBStorage dbs = new AppConfig.DBStorage();
    sparkClusters = new ArrayList<>();
    VirtualSparkClusterSpec v = new VirtualSparkClusterSpec();
    v.setId("c1001");
    v.setWeight(100);
    List<String> queues = new ArrayList<>();
    queues.add("bifrost");
    v.setQueues(queues);
    v.setEksCluster("og60-spark-eks-10");
    v.setMasterUrl("https://D8E4510D28DDB652085121B474405687.yl4.us-west-2.eks.amazonaws.com");
    v.setCaCertDataSOPS(
        "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJeU1Ea3lNekF6TlRJMU5sb1hEVE15TURreU1EQXpOVEkxTmxvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTG5mCjdIaDJuS0VreTh6V2ZUcVgya2g2TVA2c1Q3VForU1ljV3VTMkJpaHNhNjRQeHBveExwK0dIUHg0YUoyelluU2UKTnQrekdGK05Sem1rT2Zlc0NsNkY2RS9MNmloUWh1aDVJZDZrY0R3VDRpbzdhKzFReWlrZEMyaSt3YjlFUm9pRQpOYjl6SXhBOExOWHRVbC9WSEJ4eExHM0RmMEhMSVF4YXJWUERUMHJEYlpkTDgyZTZFSCt4OG5KMEVHQktGVDNVCkJTMWt2dlJyMGpPRU9lbWk0L2pRUG95OVk5RFZBUUF1RlVSODhtWGlDL0VoV0I4QXEzR0YvYUxiMWxzbDI3RlAKN1ladUtKVUxDMnNhbWJNNmdlazBpOHZIODFSenNOZlJnb3NLWExsekdheFR0VlZ4SU5zNk9ZWU4rcittaUZRLwpqenVlYUlQdUhxOEJGaC9QZTBNQ0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZJRkI1NTBLMGlIRFQ0STNWZWgwSmJGeDBjRjFNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBRzVkeldQRjJ0K2xHUjVGZlo2LwpLQjk0dzQzclNZZm1rcW1DUEQ5M0hrUHptUUdUNm1rYy9tRGUxbHdxanpmUXZnbjVxZzVqbzBHeDBTcS93UXF1CkN3MXNKekJYWVNFL3JCRVZ6Y25VVXRzcW84YzUvWnlFTWw1OVNZZ3ZNK2tYVkpoYSs5MkhncmN5WndaZnBncE4KZ25IMHFMbzFnZUVTNk4wM3YvQlhyZlJ5aS84dXBEeVJzV1VTeVdpQXhycitNb0t6WUVsTkVrbEd0RjJZSHA3NApQU3FaRW0vQWdWMDQzdEYrZVQ0UkJ1dEU5TUxIdVdRZUZvcjMyZjk1K1JMaDF1eUs4Q1hPQVVQa05jQWpweU5VCjNsYnZtbkxuZE0yQ1RZMG9RYk16Y0dzVGxkVm9sYWUwSVRyaW1raElzd2lYZUFCcHF1dUExK1BZTkVjb0lTaUEKZWpvPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==");
    v.setUserName("spark-operator-api-user");
    v.setUserTokenSOPS(
        "eyJhbGciOiJSUzI1NiIsImtpZCI6IllfQXBxTnBDUTFmX1ZzZkFMMHY0LXhvbG5jbHNDZ0pkUU1ySjRUQTJqbjQifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6InNwYXJrLW9wZXJhdG9yLWFwaS11c2VyLWJpZnJvc3QtYnBpYS10b2tlbiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJzcGFyay1vcGVyYXRvci1hcGktdXNlci1iaWZyb3N0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiY2FjMDJjNGUtZDUyYy00MjM1LWExYWEtNzAzNzYxYWRkNTgyIiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6c3Bhcmstb3BlcmF0b3ItYXBpLXVzZXItYmlmcm9zdCJ9.X6yiaS7xBqfZZvBninHBR7hvmbZSeE_ScMpE_p0esILH0yC-K19jnRPCMEkqfDJX1M1wVoLwmx1v7SYS4WFeWCkDWNDa0hjcgAlMNoaycBkgYdTQfEuiBhe57jMYLUQ-HuUTnoqf4qib9z681yQVm81ypctKkzdjpY3m949zE6po_233__sLN3zLCuTza86wgwyHmog02nSSeM-KMvmzaKeLJDvT_LA1MAdclRX0ZVrw1iQDgsOboQgt3P9MrT-PxtE1wGb_tMzlqtM05El0LHy9WLd0FOJ2FS21o1gO0DyZtXIldMuFiknCtzLnAQN6nblnJeBvue36aruPygi9gg");
    v.setSparkApplicationNamespace("spark-applications");
    v.setSparkServiceAccount("spark");
    sparkClusters.add(v);
    appconfi.setSparkClusters(sparkClusters);

    actorAssumabilityCheckRest = new ActorAssumabilityCheckRest(appconfi, meterRegistry);
  }

  // whole process walk through except for getting the dsid from ldap
  @Test
  public void testActorAssumabilityCheck() {
    Assert.assertNotNull(actorAssumabilityCheckRest);
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    Response response =
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("mingcui_yang"));
    Assert.assertEquals(response.getStatus(), 200);
    NotaryValidateResponse notaryValidateResponse = (NotaryValidateResponse) response.getEntity();
    Assert.assertFalse(notaryValidateResponse.getAssumable());
  }

  @Test
  public void testActorAssumabilityCheck_notaryRequest() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    Response response =
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("notary"));
    Assert.assertEquals(response.getStatus(), 200);
    NotaryValidateResponse notaryValidateResponse = (NotaryValidateResponse) response.getEntity();
    Assert.assertFalse(notaryValidateResponse.getAssumable());
  }

  @Test
  public void testActorAssumabilityCheck_noActorAprn() {
    String requestBodyjson =
        "{\"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    try {
      Response response =
          actorAssumabilityCheckRest.assumabilityCheckStatus(
              requestBodyjson, "application/json", new User("mingcui_yang"));
    } catch (WebApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 400);
      Assert.assertEquals(
          e.getMessage(),
          "Invalid notary assumability check request, missing Assume Aprn or Actor Aprn or ActorMetadata");
    }
  }

  @Test
  public void testActorAssumabilityCheck_noAssumeAprn() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    try {
      Response response =
          actorAssumabilityCheckRest.assumabilityCheckStatus(
              requestBodyjson, "application/json", new User("mingcui_yang"));
    } catch (WebApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 400);
      Assert.assertEquals(
          e.getMessage(),
          "Invalid notary assumability check request, missing Assume Aprn or Actor Aprn or ActorMetadata");
    }
  }

  @Test
  public void testActorAssumabilityCheck_noActorMetadata() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\",\"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\", \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    try {
      Response response =
          actorAssumabilityCheckRest.assumabilityCheckStatus(
              requestBodyjson, "application/json", new User("mingcui_yang"));
    } catch (WebApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 400);
      Assert.assertEquals(
          e.getMessage(),
          "Invalid notary assumability check request, missing Assume Aprn or Actor Aprn or ActorMetadata");
    }
  }

  @Test
  public void testActorAssumabilityCheck_emptyPodName() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\",\"actorMetadata\": {\"podName\": null, \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    Response response =
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("mingcui_yang"));
    Assert.assertEquals(response.getStatus(), 200);
    NotaryValidateResponse notaryValidateResponse = (NotaryValidateResponse) response.getEntity();
    Assert.assertFalse(notaryValidateResponse.getAssumable());
  }

  @Test
  public void testActorAssumabilityCheck_emptyPodUid() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:2701123238\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": null}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    Response response =
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("mingcui_yang"));
    Assert.assertEquals(response.getStatus(), 200);
    NotaryValidateResponse notaryValidateResponse = (NotaryValidateResponse) response.getEntity();
    Assert.assertFalse(notaryValidateResponse.getAssumable());
  }

  @Test
  public void testActorAssumabilityCheck_emptyAssumeAprn() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    Response response =
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("mingcui_yang"));
    Assert.assertEquals(response.getStatus(), 200);
    NotaryValidateResponse notaryValidateResponse = (NotaryValidateResponse) response.getEntity();
    Assert.assertFalse(notaryValidateResponse.getAssumable());
  }

  @Test
  public void testRateLimiter() {
    String requestBodyjson =
        "{\"actorAprn\": \"aprn:apple:aws:us-west-2:138716490107:eks-service-account:k8s-cluster/k8s-namespace/service-account\", \"assumeAprn\": \"aprn:apple:turi::notary:user:\",\"actorMetadata\": {\"podName\": \"c1001-7a223508a17141178cce990731a4f074-driver\", \"podUid\": \"63b91670-5438-48fa-8af4-12a22ef613d0\"}, \"actorAudience\": null,\"actorClaims\": null,\"audience\": [\"aprn:apple:turi::notary:application-group:turi-platform\",\"aprn:apple:turi::notary:application-group:polymer\"],\"sourceIp\": \"172.19.198.80\", \"claims\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
    try {
      for (int i = 0; i <= 1100; i++) {
        actorAssumabilityCheckRest.assumabilityCheckStatus(
            requestBodyjson, "application/json", new User("mingcui_yang"));
      }
    } catch (Exception e) {
      Assert.assertEquals(
          e.getMessage(),
          "Too many requests for /notary/actor-assumability-check endpoint (limit: 1000 requests per second)");
    }
  }
}
