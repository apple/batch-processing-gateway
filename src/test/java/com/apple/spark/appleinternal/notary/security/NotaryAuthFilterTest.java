package com.apple.spark.appleinternal.notary.security;

import static org.mockito.Mockito.when;

import com.apple.spark.AppConfig;
import com.apple.spark.appleinternal.notary.NotaryConstants;
import com.apple.spark.appleinternal.notary.NotaryDirectoryService;
import com.apple.spark.core.Constants;
import com.apple.spark.security.User;
import com.apple.spark.security.UserUnauthorizedHandler;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testng.Assert;

@RunWith(MockitoJUnitRunner.class)
public class NotaryAuthFilterTest {
  @Mock private ContainerRequestContext requestContext;
  private NotaryAuthFilter<User> notaryAuthFilter;
  private UriInfo uriInfo;
  private static AppConfig appconfi;

  @Before
  public void setUp() {
    Set<String> allowedUsers =
        Set.of("mingcui_yang", "allowedUsers1", "allowedUsers2", "allowedUsers3");
    Set<String> blockedUsers = Set.of("blockedUsers1", "blockedUsers2", "blockedUsers3");
    appconfi = new AppConfig();
    AppConfig.NotaryAppConfig notary = new AppConfig.NotaryAppConfig();
    notary.setTuriDirectoryApiKey("BJXDGZRWbkN6sazVRHvdnY9F");
    List<Long> proxyAllowedGroups = new ArrayList<>();
    proxyAllowedGroups.add(8609568L);
    notary.setProxyAllowedGroupIDList(proxyAllowedGroups);
    appconfi.setNotary(notary);
    NotaryDirectoryService notaryDirectoryService = new NotaryDirectoryService(appconfi);
    notaryAuthFilter =
        new NotaryAuthFilter.Builder<User>()
            .setNotaryDirectoryService(notaryDirectoryService)
            .setAuthenticator(new NotaryUserNameAuthenticator(allowedUsers, blockedUsers))
            .setRealm(Constants.REALM)
            .setUnauthorizedHandler(new UserUnauthorizedHandler())
            .buildAuthFilter();
    uriInfo =
        new UriInfo() {
          @Override
          public String getPath() {
            return "actor-assumability-check";
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
            return null;
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
  public void testGetUserFromHeaderKey_notaryPerson() throws IOException {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_PERSON_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_USER_HEADER_KEY))
        .thenReturn("allowedUsers1");
    notaryAuthFilter.filter(requestContext);
  }

  @Test
  public void testGetUserFromHeaderKey_notaryA3() throws IOException {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_CLAIMS_HEADER_KEY))
        .thenReturn(
            "{\"X-Notary-App-Person-Id\":\"2701123238\", \"X-Notary-Acaccountname\":\"mingcui_yang\"}");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    notaryAuthFilter.filter(requestContext);
  }

  @Test
  public void testGetUserFromHeaderKey_notaryA3_notAllowedUser() throws IOException {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_CLAIMS_HEADER_KEY))
        .thenReturn(
            "{\"X-Notary-App-Person-Id\":\"123457\", \"X-Notary-Acaccountname\":\"mingcui_yang\"}");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    try {
      notaryAuthFilter.filter(requestContext);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "HTTP 401 Unauthorized");
    }
  }

  @Test
  public void testGetUserFromHeaderKey_dawPersonOrA3() throws IOException {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(null);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_USER_HEADER_KEY))
        .thenReturn("allowedUsers1");
    notaryAuthFilter.filter(requestContext);
  }

  @Test
  public void testNotaryServiceUser() throws IOException {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_SOURCE_APPLICATION_KEY))
        .thenReturn("notary");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    notaryAuthFilter.filter((requestContext));
  }

  @Test
  public void testNonNotaryApplication() {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "false");
    try {
      notaryAuthFilter.filter((requestContext));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "HTTP 401 Unauthorized");
    }
  }

  @Test
  public void testEmptyUser() {
    System.setProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME, "true");
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY))
        .thenReturn(null);
    when(requestContext.getHeaderString(NotaryConstants.NOTARY_USER_HEADER_KEY)).thenReturn(null);
    try {
      notaryAuthFilter.filter((requestContext));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "HTTP 401 Unauthorized");
    }
  }
}
