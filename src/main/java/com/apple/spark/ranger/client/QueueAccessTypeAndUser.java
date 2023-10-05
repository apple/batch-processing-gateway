package com.apple.spark.ranger.client;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class QueueAccessTypeAndUser {
  private final String queue;
  private final String accessType;
  private final String user;

  public QueueAccessTypeAndUser(String queue, String accessType, String user) {
    this.queue = queue;
    this.accessType = accessType;
    this.user = user;
  }

  public String getAccessType() {
    return accessType;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(queue).append(accessType).append(user).toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    QueueAccessTypeAndUser queueAccessTypeAndUser = (QueueAccessTypeAndUser) o;
    return queue.equals(queueAccessTypeAndUser.getQueue())
        && accessType.equals(queueAccessTypeAndUser.getAccessType())
        && user.equals(queueAccessTypeAndUser.getUser());
  }
}
