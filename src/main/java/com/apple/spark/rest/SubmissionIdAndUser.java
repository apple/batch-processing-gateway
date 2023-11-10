package com.apple.spark.rest;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class SubmissionIdAndUser {
  private String submissionId;
  private String user;

  public SubmissionIdAndUser(String submissionId, String user) {
    this.submissionId = submissionId;
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  public String getSubmissionId() {
    return submissionId;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(submissionId).append(user).toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    SubmissionIdAndUser submissionIdAndUser = (SubmissionIdAndUser) o;
    return submissionId.equals(submissionIdAndUser.getSubmissionId())
        && user.equals(submissionIdAndUser.getUser());
  }
}
