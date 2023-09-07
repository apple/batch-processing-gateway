package com.apple.spark.appleinternal.notary;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotaryValidateRequestBody {

  // actorAprn: required, aprn of the actor identity
  public String actorAprn;

  // assumeAprn: aprn of the assume identity
  public String assumeAprn;
  // audience: required, list of audience aprn requested
  public List<String> audience;
  // sourceIp: IP address of the original assume request
  public String sourceIp;
  // claims: optional, list of custom claims requested
  public Map<String, String> claims;
  public Map<String, String> actorClaims;
  public Map<String, String> actorMetadata;
  public List<String> actorAudience;

  public String getActorAprn() {
    return actorAprn;
  }

  public void setActorAprn(String actorAprn) {
    this.actorAprn = actorAprn;
  }

  public String getAssumeAprn() {
    return assumeAprn;
  }

  public void setAssumeAprn(String assumeAprn) {
    this.assumeAprn = assumeAprn;
  }

  public List<String> getAudience() {
    return audience;
  }

  public void setAudience(List<String> audience) {
    this.audience = audience;
  }

  public String getSourceIp() {
    return sourceIp;
  }

  public void setSourceIp(String sourceIp) {
    this.sourceIp = sourceIp;
  }

  public Map<String, String> getClaims() {
    return claims;
  }

  public void setClaims(Map<String, String> claims) {
    this.claims = claims;
  }

  public List<String> getActorAudience() {
    return actorAudience;
  }

  public void setActorAudience(List<String> actorAudience) {
    this.actorAudience = actorAudience;
  }

  public Map<String, String> getActorClaims() {
    return actorClaims;
  }

  public void setActorClaims(Map<String, String> actorClaims) {
    this.actorClaims = actorClaims;
  }

  public Map<String, String> getActorMetadata() {
    return actorMetadata;
  }

  public void setActorMetadata(Map<String, String> actorMetadata) {
    this.actorMetadata = actorMetadata;
  }
}
