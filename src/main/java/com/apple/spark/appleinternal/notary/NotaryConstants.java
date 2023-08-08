package com.apple.spark.appleinternal.notary;

import java.util.List;

/** Constants only used for notary service of BPG. */
public class NotaryConstants {
  public static final String NOTARY_USER_HEADER_KEY = "X-Notary-Acaccountname";
  public static final String NOTARY_CLAIMS_USER_HEADER_KEY = "X-Notary-Acaccountname";
  public static final String NOTARY_IDENTITY_TYPE_HEADER_KEY = "X-Notary-Identitytype";
  public static final String NOTARY_APPLICATION_IDENTITY_TYPE = "application";
  public static final String NOTARY_PERSON_IDENTITY_TYPE = "person";
  public static final String NOTARY_CLAIMS_HEADER_KEY = "X-Notary-Claims";
  public static final String NOTARY_APPLICATION_SYSTEM_PROPERTY_NAME = "notaryApplication";
  public static final String NOTARY_SOURCE_APPLICATION_KEY = "X-Notary-Applicationid";
  public static final String NOTARY_ADMIN_USER_NAME = "notary";
  public static final String NOTARY_CHECK_ENDPOINT = "actor-assumability-check";
  public static final String NOTARY_PERSON_ID_KEY = "X-Notary-Personid";
  public static final String NOTARY_APP_PERSON_ID_KEY = "X-Notary-App-Person-Id";
  public static final String NOTARY_A3_PERSON_ID_KEY = "X-A3-Person-Id";
  public static final String NOTARY_ACTOR_ASSUME_PERSON_ID_KEY = "NOTARY_ACTOR_ASSUME_PERSON_ID";
  public static final String NOTARY_APP_NAME = "notary";
  public static final List<String> NOTARY_ADMIN_USERS = List.of(NOTARY_ADMIN_USER_NAME);
  public static final String BPG_NOTARY_TOKEN_ENV_VAR = "NOTARY_ACTOR_TOKEN";
  public static final int PERMITS_PER_SECOND = 100;
  public static final int BPG_NOTARY_CACHE_TTL = 120;
  public static final String SERVICE_ABBR = "skate";
  public static final String ASSUMABILITY_CHECK_LATENCY_METRIC_NAME =
      String.format("statsd.%s.assumability_check.latency", SERVICE_ABBR);
  public static final String ASSUMABILITY_CHECK_REQUEST_METRIC_NAME =
      String.format("statsd.%s.assumability_check.request", SERVICE_ABBR);
  public static final String ASSUMABILITY_CHECK_CACHE_LOAD =
      String.format("statsd.%s.assumability_check.status_cache.load", SERVICE_ABBR);
  public static final String ASSUMABILITY_CHECK_CACHE_LOAD_FAILURE =
      String.format("statsd.%s.assumability_check.status_cache.load_failure", SERVICE_ABBR);
}
