/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark.security;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authenticator that takes in a list of allowed users, and a list of blocked users, and ensure
 * the user is legitimate.
 */
public class UserNameBasicAuthenticator implements Authenticator<BasicCredentials, User> {

  private static final Logger logger = LoggerFactory.getLogger(UserNameBasicAuthenticator.class);

  private final Set<String> allowedUsers;
  private final Set<String> blockedUsers;

  public UserNameBasicAuthenticator(
      Collection<String> allowedUsers, Collection<String> blockedUsers) {
    this.allowedUsers = allowedUsers == null ? Collections.emptySet() : new HashSet<>(allowedUsers);
    this.blockedUsers = blockedUsers == null ? Collections.emptySet() : new HashSet<>(blockedUsers);
  }

  /** Ensure the user is not in blocked users list, and in allowed users list (can be wildcard) */
  @Override
  public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
    if (blockedUsers.contains(credentials.getUsername())) {
      logger.info("User {} is blocked", credentials.getUsername());
      return Optional.empty();
    }

    if (allowedUsers.contains("*") || allowedUsers.contains(credentials.getUsername())) {
      return Optional.of(new User(credentials.getUsername()));
    }

    return Optional.empty();
  }
}
