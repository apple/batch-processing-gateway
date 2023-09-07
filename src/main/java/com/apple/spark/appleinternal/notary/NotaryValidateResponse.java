package com.apple.spark.appleinternal.notary;

import java.util.List;
import java.util.Map;

public class NotaryValidateResponse {
  // assumable: required; true, only when actor assumed user is equal to actual user, else false.
  public Boolean assumable;
  // audience: required; use the audience list sent in the request.
  public List<String> audience;

  public Map<String, String> claims;

  public Boolean getAssumable() {
    return assumable;
  }

  public void setAssumable(Boolean assumable) {
    this.assumable = assumable;
  }

  public List<String> getAudience() {
    return audience;
  }

  public void setAudience(List<String> audience) {
    this.audience = audience;
  }

  public Map<String, String> getClaims() {
    return claims;
  }

  public void setClaims(Map<String, String> claims) {
    this.claims = claims;
  }
}
