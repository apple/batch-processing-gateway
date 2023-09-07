package com.apple.spark.appleinternal.notary.security;

import com.apple.spark.appleinternal.notary.NotaryConstants;
import com.apple.spark.appleinternal.notary.NotaryDirectoryService;
import com.apple.turi.directory.DirectoryServiceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import java.io.IOException;
import java.security.Principal;
import java.util.*;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;
import org.apache.commons.lang3.StringUtils;

public class NotaryAuthFilter<P extends Principal> extends AuthFilter<BasicCredentials, P> {
  private NotaryDirectoryService notaryDirectoryService;

  public NotaryAuthFilter(NotaryDirectoryService notaryDirectoryService) {
    this.notaryDirectoryService = notaryDirectoryService;
  }

  /**
   * Filter method called before a request has been dispatched to a resource. by the pre-match
   * response filter chain.
   *
   * @param requestContext request context.
   * @throws IOException if an I/O exception occurs.
   */
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    BasicCredentials credentials = null;
    try {
      credentials = getNotaryCredentials(requestContext);
    } catch (DirectoryServiceException e) {
      throw new WebApplicationException(e);
    }
    // @TODO:  Remove this log statement or make it debug
    logger.debug("Notary Credentials: {}", credentials);

    if (!authenticate(requestContext, credentials, SecurityContext.BASIC_AUTH)) {
      throw new WebApplicationException(unauthorizedHandler.buildResponse(this.prefix, this.realm));
    }
  }

  public static class Builder<P extends Principal>
      extends AuthFilterBuilder<BasicCredentials, P, NotaryAuthFilter<P>> {
    private NotaryDirectoryService notaryDirectoryService;

    public Builder() {}

    public Builder<P> setNotaryDirectoryService(NotaryDirectoryService notaryDirectoryService) {
      this.notaryDirectoryService = notaryDirectoryService;
      return this;
    }

    @Override
    protected NotaryAuthFilter<P> newInstance() {
      return new NotaryAuthFilter<>(this.notaryDirectoryService);
    }
  }

  @Nullable
  private BasicCredentials getNotaryCredentials(ContainerRequestContext requestContext)
      throws DirectoryServiceException {
    boolean isNotaryApplication = isNotaryEnabled();
    String username = "";
    String password = "";

    // If bpg is a registered notary application, then get user from header key
    if (isNotaryApplication) {
      Optional<String> user = getUserFromHeaderKey(requestContext);
      if (user.isPresent() && StringUtils.isNotBlank(user.get())) {
        username = user.get();
      } else {
        logger.error("Notary invalid user credentials {}", user);
        return null;
      }

    } else {
      return null;
    }

    return new BasicCredentials(username, password);
  }

  /**
   * For X-Notary-IdentityType = person, user-header key is X-Notary-Acaccountname. For
   * X-Notary-IdentityType = application , check if source application is Notary application. If
   * not, get user identity from X-Notary-Claims. Notary forwards the header key
   * X-Notary-Acaccountname for Notary Person, DAW person and A3 application identity type.
   *
   * @param requestContext the container request context containing the necessary request
   *     information.
   * @return username retrieved from request.
   */
  private Optional<String> getUserFromHeaderKey(ContainerRequestContext requestContext)
      throws DirectoryServiceException {
    Optional<String> identityType =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY);
    Optional<String> userName;

    logger.debug("Notary Identity Type: {}", identityType);

    if (identityType.isPresent()) {
      // notary app
      if (identityType.get().equals(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE)) {
        userName = getNotaryApplicationUser(requestContext);
      } else {
        // notary person
        userName = getHeaderValue(requestContext, NotaryConstants.NOTARY_USER_HEADER_KEY);
      }
    } else {
      // daw person or A3
      userName = getHeaderValue(requestContext, NotaryConstants.NOTARY_USER_HEADER_KEY);
    }
    return userName;
  }

  /**
   * For notary app-to-app requests, extract Person Id of the application submitted requests from
   * the Notary claims header object. 'X-Notary-Claims: {'X-Notary-App-Person-Id': '12345',
   * 'X-Notary-Acaccountname': 'mingcui_yang'}
   *
   * @param requestContext the request context from which to extract the Notary claims header.
   * @return an Optional containing the personId if found, or empty Optional otherwise.
   */
  private Optional<Long> getApplicationPersonIdFromClaims(ContainerRequestContext requestContext) {
    // Get the value of the 'X-Notary-Claims' header
    Optional<String> notaryClaims =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_CLAIMS_HEADER_KEY);
    logger.debug("NotaryClaims: {}", notaryClaims);
    // Check if 'notaryClaimsHeader' is present or empty
    if (notaryClaims.isEmpty()) {
      logger.debug("Notary claims header is empty");
      return Optional.empty();
    }

    // Parse the JSON string to a JSON object
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonObj;
    try {
      jsonObj = objectMapper.readTree(notaryClaims.get());
    } catch (IOException e) {
      logger.error("Error parsing notary claims header", e);
      return Optional.empty();
    }
    JsonNode personIdJsonNode = jsonObj.get(NotaryConstants.NOTARY_APP_PERSON_ID_KEY);
    if (personIdJsonNode == null || !personIdJsonNode.isTextual()) {
      logger.error("Notary claims header does not contain user");
      return Optional.empty();
    }
    String personId = personIdJsonNode.asText();
    return Optional.of(Long.parseLong(personId));
  }

  /**
   * Retrieves the username of the authenticated user in the notary application.
   *
   * @param requestContext The container request context containing the necessary request
   *     information
   * @return An Optional<String> object containing the username if present, or an empty Optional
   *     otherwise
   */
  private Optional<String> getNotaryApplicationUser(ContainerRequestContext requestContext)
      throws DirectoryServiceException {
    // Default notary admin user
    Optional<String> notaryAdminUserName = Optional.of(NotaryConstants.NOTARY_ADMIN_USER_NAME);
    Optional<String> userName = Optional.empty();

    if (isNotaryApplication(requestContext)) {
      // If it's a request from Notary application, use the default notary app admin username
      userName = notaryAdminUserName;
    } else {
      Optional<Long> personId = getApplicationPersonIdFromClaims(requestContext);
      if (notaryDirectoryService.checkIfPersonIdInAllowedGroups(personId.get())) {
        // Retrieve the authenticated user from the claims header of the request
        userName = getApplicationUserFromClaims(requestContext);
      }
    }

    return userName;
  }

  /**
   * For notary app-to-app requests, extract Person Identity of the application submitted requests
   * from the Notary claims header object. 'X-Notary-Claims: {'X-Notary-App-Person-Id': '12345',
   * 'X-Notary-Acaccountname': 'mingcui_yang'}
   *
   * @param requestContext the request context from which to extract the Notary claims header.
   * @return an Optional containing the user if found, or empty Optional otherwise.
   */
  private Optional<String> getApplicationUserFromClaims(ContainerRequestContext requestContext) {
    // Get the value of the 'X-Notary-Claims' header
    Optional<String> notaryClaims =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_CLAIMS_HEADER_KEY);
    logger.debug("NotaryClaims: {}", notaryClaims);
    // Check if 'notaryClaimsHeader' is present or empty
    if (notaryClaims.isEmpty()) {
      logger.debug("Notary claims header is empty");
      return Optional.empty();
    }

    // Parse the JSON string to a JSON object
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonObj;
    try {
      jsonObj = objectMapper.readTree(notaryClaims.get());
    } catch (IOException e) {
      logger.error("Error parsing notary claims header", e);
      return Optional.empty();
    }
    JsonNode userNameJsonNode = jsonObj.get(NotaryConstants.NOTARY_CLAIMS_USER_HEADER_KEY);
    if (userNameJsonNode == null || !userNameJsonNode.isTextual()) {
      logger.error("Notary claims header does not contain user");
      return Optional.empty();
    }
    String userName = userNameJsonNode.asText();
    return Optional.of(userName);
  }

  /**
   * Check if the incoming application is Notary application(Notary service).
   *
   * @param requestContext the request context from Notary application, which doesn't have 'Claims'
   *     field.
   * @return boolean, indicates if the application is Notary application
   */
  private boolean isNotaryApplication(ContainerRequestContext requestContext) {
    Optional<String> applicationId =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_SOURCE_APPLICATION_KEY);
    String endPointStr = requestContext.getUriInfo().getPath();
    logger.info("ApplicationId: {} :: Endpoint: {}", applicationId, endPointStr);

    if (applicationId.isEmpty()) {
      return false;
    }
    // Allow Notary application to access actor-assumability-check endpoint as notary service
    // application without claims.
    return applicationId.get().equals(NotaryConstants.NOTARY_APP_NAME)
        && endPointStr.contains(NotaryConstants.NOTARY_CHECK_ENDPOINT);
  }

  /**
   * Checks if the application is a notary Application by checking for a system property.
   *
   * @return true if the Notary Application system property is not blank and is set to true, false
   *     otherwise.
   */
  private boolean isNotaryEnabled() {
    String notaryAppProperty =
        System.getProperty(NotaryConstants.NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME);
    return StringUtils.isNotBlank(notaryAppProperty) && Boolean.parseBoolean(notaryAppProperty);
  }

  /**
   * Retrieves the value of a specified HTTP header from the provided ContainerRequestContext
   * object.
   *
   * @param requestContext the ContainerRequestContext object containing information about the
   *     current HTTP request.
   * @param headerName the name of the header whose value is being retrieved.
   * @return an Optional object containing the value of the header if it exists, or an empty
   *     Optional if it does not.
   */
  private Optional<String> getHeaderValue(
      ContainerRequestContext requestContext, String headerName) {
    return Optional.ofNullable(requestContext.getHeaderString(headerName));
  }
}
