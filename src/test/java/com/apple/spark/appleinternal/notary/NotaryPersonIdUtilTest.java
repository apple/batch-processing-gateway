package com.apple.spark.appleinternal.notary;

import static org.mockito.Mockito.when;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testng.Assert;

@RunWith(MockitoJUnitRunner.class)
public class NotaryPersonIdUtilTest {
  @Mock private ContainerRequestContext requestContext;
  private static SparkApplicationSpec sparkSpec;
  private static AppConfig appconfi;
  private static UriInfo uriInfo;

  @Before
  public void setUp() {
    appconfi = new AppConfig();
    AppConfig.NotaryAppConfig notary = new AppConfig.NotaryAppConfig();
    notary.setAppNameSpace("bpg-siri-aws-test");
    appconfi.setNotary(notary);
    sparkSpec = new SparkApplicationSpec();
    sparkSpec.setDriver(new DriverSpec());
    sparkSpec.setExecutor(new ExecutorSpec());
    uriInfo =
        new UriInfo() {
          @Override
          public String getPath() {
            return null;
          }

          @Override
          public String getPath(boolean decode) {
            return null;
          }

          @Override
          public List<PathSegment> getPathSegments() {
            return null;
          }

          @Override
          public List<PathSegment> getPathSegments(boolean decode) {
            return null;
          }

          @Override
          public URI getRequestUri() {
            try {
              return new URI("https://batch-gateway-test-03.aws.sea.g.apple.com/skatev2-notary");
            } catch (URISyntaxException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public UriBuilder getRequestUriBuilder() {
            return null;
          }

          @Override
          public URI getAbsolutePath() {
            return null;
          }

          @Override
          public UriBuilder getAbsolutePathBuilder() {
            return null;
          }

          @Override
          public URI getBaseUri() {
            return null;
          }

          @Override
          public UriBuilder getBaseUriBuilder() {
            return null;
          }

          @Override
          public MultivaluedMap<String, String> getPathParameters() {
            return null;
          }

          @Override
          public MultivaluedMap<String, String> getPathParameters(boolean decode) {
            return null;
          }

          @Override
          public MultivaluedMap<String, String> getQueryParameters() {
            return null;
          }

          @Override
          public MultivaluedMap<String, String> getQueryParameters(boolean decode) {
            return null;
          }

          @Override
          public List<String> getMatchedURIs() {
            return null;
          }

          @Override
          public List<String> getMatchedURIs(boolean decode) {
            return null;
          }

          @Override
          public List<Object> getMatchedResources() {
            return null;
          }

          @Override
          public URI resolve(URI uri) {
            return null;
          }

          @Override
          public URI relativize(URI uri) {
            return null;
          }
        };
  }

  @Test
  public void testExtractPersonId_notaryApp() {
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_CLAIMS_HEADER_KEY))
        .thenReturn(
            "{\"X-Notary-App-Person-Id\":\"1234567\", \"X-Notary-Acaccountname\":\"allowedUsers1\"}");
    String personId = NotaryPersonIdUtil.extractPersonId(requestContext).get();
    Assert.assertEquals(personId, "1234567");
  }

  @Test
  public void testExtractPersonId_notaryPerson() {
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_PERSON_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_PERSON_ID_KEY))
        .thenReturn("1234567");
    String personId = NotaryPersonIdUtil.extractPersonId(requestContext).get();
    Assert.assertEquals(personId, "1234567");
  }

  @Test
  public void testSetPersonId_notaryPerson() {
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_PERSON_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_PERSON_ID_KEY))
        .thenReturn("1234567");
    NotaryPersonIdUtil.setPersonId(requestContext, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNotNull(envVars);
    Assert.assertEquals(envVars.get(0).getValue(), "1234567");
  }

  @Test
  public void testSetPersonId_dawPerson() {
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn("");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_PERSON_ID_KEY))
        .thenReturn("1234567");
    NotaryPersonIdUtil.setPersonId(requestContext, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNotNull(envVars);
    Assert.assertEquals(envVars.get(0).getValue(), "1234567");
  }

  @Test
  public void testSetPersonId_notaryApp() {
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_CLAIMS_HEADER_KEY))
        .thenReturn(
            "{\"X-Notary-App-Person-Id\":\"1234567\", \"X-Notary-Acaccountname\":\"allowedUsers1\"}");
    NotaryPersonIdUtil.setPersonId(requestContext, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNotNull(envVars);
    Assert.assertEquals(envVars.get(0).getValue(), "1234567");
  }

  @Test
  public void testSetPersonId_A3() {
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn("");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_PERSON_ID_KEY)).thenReturn("");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_PERSON_ID_KEY))
        .thenReturn("1234567");
    NotaryPersonIdUtil.setPersonId(requestContext, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNotNull(envVars);
    Assert.assertEquals(envVars.get(0).getValue(), "1234567");
  }
}
