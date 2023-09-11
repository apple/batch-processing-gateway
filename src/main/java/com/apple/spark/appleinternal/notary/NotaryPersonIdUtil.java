package com.apple.spark.appleinternal.notary;

import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotaryPersonIdUtil {
  private static final Logger logger = LoggerFactory.getLogger(NotaryPersonIdUtil.class);

  /**
   * Retrieves the value of PersonId from the ContainerRequestContext of submission request and set
   * it as env variable. Need personId in assumability check.
   *
   * @param requestContext the ContainerRequestContext object containing information about the
   *     current HTTP request.
   * @param sparkSpec the environment that the personId need to be set in.
   */
  public static void setPersonId(
      ContainerRequestContext requestContext, @NotNull SparkApplicationSpec sparkSpec) {
    // get personId from request header
    Optional<String> personId = extractPersonId(requestContext);
    // set personId as env variable of driver pod and executor pod
    EnvVar personIdEnvVar =
        new EnvVar(NotaryConstants.NOTARY_ACTOR_ASSUME_PERSON_ID_KEY, personId.get());

    if (sparkSpec.getDriver() == null) {
      sparkSpec.setDriver(new DriverSpec());
    }
    if (sparkSpec.getDriver().getEnv() == null) {
      sparkSpec.getDriver().setEnv(new ArrayList<EnvVar>());
    }
    sparkSpec.getDriver().getEnv().add(personIdEnvVar);

    if (sparkSpec.getExecutor() == null) {
      sparkSpec.setExecutor(new ExecutorSpec());
    }
    if (sparkSpec.getExecutor().getEnv() == null) {
      sparkSpec.getExecutor().setEnv(new ArrayList<EnvVar>());
    }
    sparkSpec.getExecutor().getEnv().add(personIdEnvVar);
  }

  /**
   * Extract the person Id from request. Extract the person ID from Claims in the header, if it's
   * notary application use case else extract directly from header "X-Notary-Personid".
   *
   * @param requestContext
   * @return Optional<String> personId
   */
  public static Optional<String> extractPersonId(ContainerRequestContext requestContext) {
    Optional<String> personId;
    Optional<String> identityType =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_IDENTITY_TYPE_HEADER_KEY);

    logger.debug("Notary Identity Type: {}", identityType);

    if (identityType.isPresent()
        && identityType.get().equals(NotaryConstants.NOTARY_APPLICATION_IDENTITY_TYPE)) {
      // notary application
      personId = getApplicationPersonIdFromClaims(requestContext);
    } else {
      // notary person, daw app and daw person
      personId = getHeaderValue(requestContext, NotaryConstants.NOTARY_PERSON_ID_KEY);
    }
    return personId;
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
  private static Optional<String> getHeaderValue(
      ContainerRequestContext requestContext, String headerName) {
    return Optional.ofNullable(requestContext.getHeaderString(headerName));
  }

  /**
   * For notary app-to-app requests, extract Person Id of the application submitted requests from
   * the Notary claims header object. 'X-Notary-Claims: {'X-Notary-App-Person-Id': '12345',
   * 'X-Notary-Acaccountname': 'mingcui_yang'}
   *
   * @param requestContext the request context from which to extract the Notary claims header.
   * @return an Optional containing the personId if found, or empty Optional otherwise.
   */
  private static Optional<String> getApplicationPersonIdFromClaims(
      ContainerRequestContext requestContext) {
    logger.debug("Request url: {}", requestContext.getUriInfo().getRequestUri().toString());
    // Get the value of the 'X-Notary-Claims' header
    Optional<String> notaryClaims =
        getHeaderValue(requestContext, NotaryConstants.NOTARY_CLAIMS_HEADER_KEY);
    logger.debug("NotaryClaims: {}", notaryClaims);
    // Check if 'notaryClaimsHeader' is present or empty
    if (notaryClaims.isEmpty()) {
      logger.warn("Notary claims header is empty !");
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
    return Optional.of(personId);
  }
}
