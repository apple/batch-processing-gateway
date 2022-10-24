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

import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JwtUtilsTest {

  @Test
  public void createToken_emptyClaim() {
    String token = JwtUtils.createToken("secret1", "issuer1", "test1", "claim1", new String[0]);
    DecodedJWT jwt = JwtUtils.verifyToken(token, "secret1");
    Assert.assertNotNull(jwt);
    Assert.assertEquals(jwt.getIssuer(), "issuer1");
    Assert.assertEquals(jwt.getSubject(), "test1");
    Claim nullClaim = jwt.getClaim("notExistingClaim");
    Assert.assertTrue(nullClaim.isNull());
    Claim claim = jwt.getClaim("claim1");
    Assert.assertFalse(claim.isNull());
    Assert.assertEquals(claim.asArray(String.class), new String[0]);
  }

  @Test
  public void createToken_singleValueClaim() {
    String token =
        JwtUtils.createToken("secret1", "issuer1", "test1", "claim1", new String[] {"v1"});
    DecodedJWT jwt = JwtUtils.verifyToken(token, "secret1");
    Claim claim = jwt.getClaim("claim1");
    Assert.assertEquals(claim.asArray(String.class), new String[] {"v1"});
  }

  @Test
  public void createToken_multiValueClaim() {
    String token =
        JwtUtils.createToken(
            "secret1", "issuer1", "test1", "claim1", new String[] {"v1", "value2"});
    DecodedJWT jwt = JwtUtils.verifyToken(token, "secret1");
    Claim claim = jwt.getClaim("claim1");
    Assert.assertEquals(claim.asArray(String.class), new String[] {"v1", "value2"});
  }

  @Test(expectedExceptions = SignatureVerificationException.class)
  public void createToken_verifyWithInvalidSecret() {
    String token =
        JwtUtils.createToken("secret1", "issuer1", "test1", "claim1", new String[] {"v1"});
    JwtUtils.verifyToken(token, "secret2");
  }
}
