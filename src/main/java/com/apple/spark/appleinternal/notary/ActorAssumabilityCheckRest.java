package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.core.Constants;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.rest.RestBase;
import com.apple.spark.security.User;
import com.apple.spark.util.ExceptionUtils;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.util.concurrent.RateLimiter;
import io.dropwizard.auth.Auth;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/notary/actor-assumability-check")
@Consumes({MediaType.APPLICATION_JSON, "text/yaml", MediaType.WILDCARD})
@Produces(MediaType.APPLICATION_JSON)
public class ActorAssumabilityCheckRest extends RestBase {
  private static final Logger logger = LoggerFactory.getLogger(ActorAssumabilityCheckRest.class);
  private RateLimiter rateLimiter;
  private Integer permit_per_second;

  public ActorAssumabilityCheckRest(AppConfig appConfig, MeterRegistry meterRegistry) {
    super(appConfig, meterRegistry);

    if (appConfig.getNotary().getAacRateLimit() != null) {
      permit_per_second = appConfig.getNotary().getAacRateLimit();
    } else {
      permit_per_second = NotaryConstants.DEFAULT_PERMITS_PER_SECOND;
    }
    logger.debug("requests allowed per second is: {}", permit_per_second);
    rateLimiter = RateLimiter.create(permit_per_second);
  }

  @POST
  @Timed
  @Operation(
      summary =
          "Check if the turi domain Narrative identity cert can be exchanged for a notary token.",
      tags = {"Assumability Check"})
  @ApiResponse(
      responseCode = "200",
      content =
          @Content(
              mediaType = "application/json",
              schema = @Schema(implementation = NotaryValidateResponse.class)))
  @ApiResponse(
      responseCode = "401",
      description = "Unauthorized access: Identity of request is not valid")
  @ApiResponse(
      responseCode = "400",
      description = "Malformed Request: Request arguments are invalid or lack required fields")
  @Consumes({MediaType.APPLICATION_JSON, "text/yaml", MediaType.WILDCARD})
  @ExceptionMetered(
      name = "WebApplicationException",
      absolute = true,
      cause = WebApplicationException.class)
  public Response assumabilityCheckStatus(
      @RequestBody(
              description = "Assumed user identity with actor",
              required = true,
              content =
                  @Content(schema = @Schema(implementation = NotaryValidateRequestBody.class)))
          String requestBody,
      @Parameter(
              description =
                  "options: application/json, text/yaml, or leave it empty for API to figure out")
          @HeaderParam("content-type")
          String contentType,
      @Parameter(hidden = true) @Auth User user) {

    checkRateForAssumabilityCheck("/notary/actor-assumability-check");
    return timerMetrics.record(
        () -> assumabilityCheck(requestBody, contentType, user),
        NotaryConstants.ASSUMABILITY_CHECK_LATENCY_METRIC_NAME,
        Tag.of("name", "assumability_check"),
        Tag.of("user", getUserTagValue(user)));
  }

  /**
   * Comparing the actor assumed user and pod of the given job to check if the turi domain Narrative
   * identity cert can be exchanged for a notary token.
   *
   * @param requestBody
   * @param contentType yaml/json
   * @param user
   * @return Response contains the NotaryValidationResponse, status and Cache header for Notary
   *     Service.
   */
  private Response assumabilityCheck(String requestBody, String contentType, User user) {
    requestCounters.increment(
        NotaryConstants.ASSUMABILITY_CHECK_REQUEST_METRIC_NAME,
        Tag.of("name", "assumability_check"),
        Tag.of("user", user.getName()));

    NotaryValidateRequestBody notaryValidateRequestBody =
        parseSubmitRequest(requestBody, contentType);

    validateRequest(notaryValidateRequestBody);

    String assumeAprn = notaryValidateRequestBody.getAssumeAprn();
    Map<String, String> claims = notaryValidateRequestBody.getClaims();
    List<String> audience = notaryValidateRequestBody.getAudience();
    Map<String, String> actorMetadata = notaryValidateRequestBody.getActorMetadata();
    Boolean validUserIdentity = checkIdentity(assumeAprn, actorMetadata);

    NotaryValidateResponse response = new NotaryValidateResponse();
    response.setAssumable(validUserIdentity);
    response.setAudience(audience); // @TODO
    response.setClaims(claims);

    return Response.ok(response).build();
  }

  /**
   * Check if the actorAprn， assumeAprn, actorMetadata is null and response with 400 if any of them
   * is null.
   *
   * @param request
   */
  private void validateRequest(NotaryValidateRequestBody request) {
    if (request.getActorAprn() == null
        || request.getAssumeAprn() == null
        || request.getActorMetadata() == null) {
      throw new WebApplicationException(
          "Invalid notary assumability check request, missing Assume Aprn or Actor Aprn or ActorMetadata",
          Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Check pod identity: Get pod metadata from k8s cluster. Compare the actual podName and podUid
   * with the podName and podUid in "actorMetadata" in request. Check user identity: Get personId
   * from Turi Directory using actual username (proxy user) in the pod spec. Compare actual personId
   * with the assumed personId.
   *
   * @param actorMetadata
   * @param assumeAprn
   * @return true if pass both pod identity check and user identity check else false.
   */
  private Boolean checkIdentity(String assumeAprn, Map<String, String> actorMetadata) {
    String actorAssumePersonId = getIdentityFromAprn(assumeAprn);
    Boolean isValidIdentity = false;

    String actorPodName = actorMetadata.get("podName");
    String actorPodUid = actorMetadata.get("podUid");

    if (actorAssumePersonId.isBlank() || actorAssumePersonId.isEmpty()) {
      logger.debug("actor assume personId is empty or blank!");
      return isValidIdentity;
    } else if (actorPodName == null || actorPodName.isBlank() || actorPodName.isEmpty()) {
      logger.debug("actor pod name is missing!");
      return isValidIdentity;
    } else if (actorPodUid == null || actorPodUid.isBlank() || actorPodUid.isEmpty()) {
      logger.debug("actor pod uid is missing!");
      return isValidIdentity;
    }

    String submissionId = getSubmissionId(actorPodName);
    if (submissionId == null || submissionId.isBlank() || submissionId.isEmpty()) {
      logger.debug("The actor pod Name is invalid!");
      return isValidIdentity;
    }
    // check pod identity
    ObjectMeta podMetadata = getPodIdentity(submissionId, actorPodName);
    if (podMetadata == null) {
      logger.debug("The pod:{} doesn't exist!", actorPodName);
      return isValidIdentity;
    }

    logger.debug("pod identity info for job: {}", podMetadata);

    // check if the pod uid matched actorPodUid
    String podUid = podMetadata.getUid();
    if (podUid == null || podUid.isEmpty() || podUid.isBlank()) {
      logger.debug("pod uid for pod {} is null or empty!", actorPodName);
      return isValidIdentity;
    } else if (actorPodUid.equals(podUid)) {
      isValidIdentity = true;
    } else {
      logger.debug("Actor pod uid doesn't match the actual pod uid!");
      return isValidIdentity;
    }

    // user identity check
    // check if the assumed user personId match the actual user personId
    String proxyUser = podMetadata.getLabels().get(Constants.PROXY_USER_LABEL);
    String delegatedPersonId =
        podMetadata.getAnnotations().get(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY);
    logger.debug("Actor assume user personId: {}", actorAssumePersonId);
    logger.debug("Spark job delegated user personId:{}", delegatedPersonId);

    if (delegatedPersonId == null || delegatedPersonId.isEmpty() || delegatedPersonId.isBlank()) {
      isValidIdentity = false;
      logger.debug("No valid delegated personId found in pod annotations for user:{}", proxyUser);
    } else if (delegatedPersonId.equals(actorAssumePersonId)) {
      isValidIdentity = true;
    } else {
      isValidIdentity = false;
      logger.debug(
          "Spark delegated personId {} does not match Notary assume user {}",
          delegatedPersonId,
          actorAssumePersonId);
    }

    return isValidIdentity;
  }

  /**
   * Return metadata of the pod if the podName is valid in the cluster specified by the
   * submissionId.
   *
   * @param submissionId
   * @param podName
   * @return ObjectMeta object of the pod if the podName is valid in the cluster specified by the
   *     submissionId else null.
   */
  @Nullable
  private ObjectMeta getPodIdentity(String submissionId, String podName) {
    VirtualSparkClusterSpec sparkCluster = getSparkCluster(submissionId);
    Pod pod = getPod(podName, sparkCluster);
    if (pod == null) {
      return null;
    }
    ObjectMeta metaData = pod.getMetadata();
    return metaData;
  }

  /**
   * Extract actorAssumePersonId/submissionId from Aprn.
   *
   * @param notaryIdentityAprn
   * @return actorAssumePersonId/submissionId
   */
  private String getIdentityFromAprn(String notaryIdentityAprn) {
    String output = "";
    String patternString = "aprn:apple:turi::(.+):(.+):(.+)";
    Pattern pattern = Pattern.compile(patternString);
    if (notaryIdentityAprn == null
        || notaryIdentityAprn.isEmpty()
        || notaryIdentityAprn.isBlank()) {
      logger.error("Error Empty aprn string");
    } else {
      Matcher matcher = pattern.matcher(notaryIdentityAprn);
      if (matcher.matches()) {
        output = matcher.group(3);
        logger.debug("Extracted identity from aprn: " + output);
      } else {
        logger.error("The format of aprn: " + notaryIdentityAprn + " is wrong!");
      }
    }
    return output;
  }

  /**
   * Get submissionId from the actor PodName.
   *
   * @param podName
   * @return submissionId String if the pod name is valid otherwise null.
   */
  private String getSubmissionId(String podName) {
    String regex = "(.*?)(?:-driver|-exec-\\d+)";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(podName);

    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  /**
   * Parse yaml/json format request into NotaryValidateRequestBody object.
   *
   * @param requestBody
   * @param contentType
   * @return NotaryValidateRequestBody object.
   */
  private static NotaryValidateRequestBody parseSubmitRequest(
      String requestBody, String contentType) {
    NotaryValidateRequestBody request;

    boolean parseRequestAsYaml =
        !StringUtils.isEmpty(contentType) && contentType.toLowerCase().contains("yaml");

    // parse request as YAML or JSON, depending on the content type
    if (parseRequestAsYaml) {
      ObjectMapper yamlObjectMapper =
          new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
      try {
        request = yamlObjectMapper.readValue(requestBody, NotaryValidateRequestBody.class);
      } catch (Throwable e) {
        logger.warn("Failed to parse request as YAML", e);
        throw new WebApplicationException(
            String.format("Invalid YAML request: %s", ExceptionUtils.getExceptionNameAndMessage(e)),
            Response.Status.BAD_REQUEST);
      }
    } else {
      // try to parse the request body as JSON
      try {
        request = new ObjectMapper().readValue(requestBody, NotaryValidateRequestBody.class);
      } catch (Throwable e) {
        throw new WebApplicationException(
            String.format("Invalid json request: %s", ExceptionUtils.getExceptionNameAndMessage(e)),
            Response.Status.BAD_REQUEST);
      }
    }

    return request;
  }

  /**
   * Return username for datadog metrics collecting.
   *
   * @param user who makes the request to assumability check endpoint.
   * @return username.
   */
  private String getUserTagValue(User user) {
    if (user != null && user.getName() != null) {
      return user.getName();
    } else {
      return "";
    }
  }

  /**
   * Rate limiter for the assumability_check endpoint. Will response with status 429 if
   * request_per_second exceeds the given rate limit value (default is 10000).
   *
   * @param endpointLogInfo
   */
  private void checkRateForAssumabilityCheck(String endpointLogInfo) {
    boolean acquired = rateLimiter.tryAcquire();
    if (!acquired) {
      throw new WebApplicationException(
          String.format(
              "Too many requests for %s endpoint (limit: %s requests per second)",
              endpointLogInfo, permit_per_second),
          Response.Status.TOO_MANY_REQUESTS);
    }
  }
}
