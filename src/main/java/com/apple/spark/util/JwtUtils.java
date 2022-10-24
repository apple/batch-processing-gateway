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

package com.apple.spark.util;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import java.util.List;

public class JwtUtils {

  public static String createToken(
      String secret, String issuer, String subject, String claimName, String[] claimItems) {
    Algorithm algorithm = Algorithm.HMAC256(secret);
    return JWT.create()
        .withIssuer(issuer)
        .withSubject(subject)
        .withArrayClaim(claimName, claimItems)
        .sign(algorithm);
  }

  public static String createToken(
      String secret, String issuer, String subject, String claimName, List<String> claimItems) {
    Algorithm algorithm = Algorithm.HMAC256(secret);
    return JWT.create()
        .withIssuer(issuer)
        .withSubject(subject)
        .withClaim(claimName, claimItems)
        .sign(algorithm);
  }

  // will throw out JWTVerificationException on invalid token
  public static DecodedJWT verifyToken(String token, String secret) {
    Algorithm algorithm = Algorithm.HMAC256(secret);
    // TODO cache and reuse verifier to improve performance
    JWTVerifier verifier = JWT.require(algorithm).build();
    DecodedJWT jwt = verifier.verify(token);
    return jwt;
  }
}
