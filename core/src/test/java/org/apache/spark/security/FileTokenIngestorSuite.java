/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.security;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;

import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FileTokenIngestorSuite {

  private Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("token-ingestor-test");
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (tempDir != null) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder())
          .forEach(path -> {
            try { Files.deleteIfExists(path); } catch (Exception e) { /* cleanup, ignore */ }
          });
    }
  }

  private String createUnsignedJwt(String subject, String issuer, Date issuedAt, Date expiresAt) {
    var builder = Jwts.builder()
        .subject(subject)
        .issuer(issuer);
    if (issuedAt != null) builder.issuedAt(issuedAt);
    if (expiresAt != null) builder.expiration(expiresAt);
    return builder.compact();
  }

  private String createUnsignedJwt(String subject, String issuer) {
    return createUnsignedJwt(subject, issuer,
        new Date(), new Date(System.currentTimeMillis() + 3600000));
  }

  private String createUnsignedJwt() {
    return createUnsignedJwt("system:serviceaccount:ns:sa",
        "https://kubernetes.default.svc");
  }

  private String createSignedJwt(String subject, String issuer) throws Exception {
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048);
    KeyPair kp = kpg.generateKeyPair();
    return Jwts.builder()
        .subject(subject)
        .issuer(issuer)
        .issuedAt(new Date())
        .expiration(new Date(System.currentTimeMillis() + 3600000))
        .signWith(kp.getPrivate())
        .compact();
  }

  private void writeToken(Path path, String content) throws Exception {
    Files.write(path, content.getBytes("UTF-8"));
  }

  @Test
  public void loadReturnsValidUserContextFromTokenFile() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = createUnsignedJwt();
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result = ingestor.load();

    assertTrue(result.isPresent());
    UserContext ctx = result.get();
    assertEquals("system:serviceaccount:ns:sa", ctx.getPrincipal());
    assertEquals("https://kubernetes.default.svc", ctx.getIssuer());
    assertEquals(token, ctx.getRawToken());
    assertNotNull(ctx.getIssuedAt());
    assertNotNull(ctx.getExpiresAt());
  }

  @Test
  public void loadWorksWithSignedJwt() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = createSignedJwt("user@corp.example.com", "https://oidc.eks.us-west-2.amazonaws.com/id/ABC123");
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result = ingestor.load();

    assertTrue(result.isPresent());
    UserContext ctx = result.get();
    assertEquals("user@corp.example.com", ctx.getPrincipal());
    assertEquals("https://oidc.eks.us-west-2.amazonaws.com/id/ABC123", ctx.getIssuer());
    assertEquals(token, ctx.getRawToken());
    assertNotNull(ctx.getIssuedAt());
    assertNotNull(ctx.getExpiresAt());
  }

  @Test
  public void loadDetectsFileRotation() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token1 = createUnsignedJwt("user1", "https://issuer.example.com");
    writeToken(tokenFile, token1);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result1 = ingestor.load();
    assertEquals("user1", result1.get().getPrincipal());

    // Simulate token rotation: sleep to ensure mtime differs
    Thread.sleep(100);
    String token2 = createUnsignedJwt("user2", "https://issuer.example.com");
    writeToken(tokenFile, token2);

    Optional<UserContext> result2 = ingestor.load();
    assertEquals("user2", result2.get().getPrincipal());
    assertEquals(token2, result2.get().getRawToken());
  }

  @Test
  public void loadReturnsEmptyForMissingFile() {
    Path tokenFile = tempDir.resolve("nonexistent");
    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadReturnsEmptyForEmptyFile() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    writeToken(tokenFile, "");

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadReturnsEmptyForWhitespaceOnlyFile() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    writeToken(tokenFile, "   \n\t  ");

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadReturnsEmptyForMalformedJwt() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    writeToken(tokenFile, "not-a-jwt");

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadReturnsEmptyForJwtMissingSubClaim() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = Jwts.builder()
        .issuer("https://issuer.example.com")
        .compact();
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadReturnsEmptyForJwtMissingIssClaim() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = Jwts.builder()
        .subject("user1")
        .compact();
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    assertTrue(ingestor.load().isEmpty());
  }

  @Test
  public void loadHandlesJwtWithoutIatAndExpClaims() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = Jwts.builder()
        .subject("user1")
        .issuer("https://issuer.example.com")
        .compact();
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result = ingestor.load();
    assertTrue(result.isPresent());
    assertEquals("user1", result.get().getPrincipal());
    assertNull(result.get().getIssuedAt());
    assertNull(result.get().getExpiresAt());
  }

  @Test
  public void loadUsesCachedResultWhenFileUnchanged() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = createUnsignedJwt("cached-user", "https://issuer.example.com");
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result1 = ingestor.load();
    Optional<UserContext> result2 = ingestor.load();

    // Reference identity proves the cached path was hit (not re-parsed)
    assertSame(result1.get(), result2.get());
    assertEquals("cached-user", result1.get().getPrincipal());
  }

  @Test
  public void loadWorksWithKubernetesProjectedToken() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = createUnsignedJwt(
        "system:serviceaccount:spark-ns:spark-driver-sa",
        "https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B716D3041E");
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result = ingestor.load();
    assertTrue(result.isPresent());
    assertEquals("system:serviceaccount:spark-ns:spark-driver-sa", result.get().getPrincipal());
    assertTrue(result.get().getIssuer().contains("oidc.eks"));
  }

  @Test
  public void loadWorksWithPerUserIdentityToken() throws Exception {
    Path tokenFile = tempDir.resolve("token");
    String token = createUnsignedJwt("user@example.com", "https://accounts.google.com");
    writeToken(tokenFile, token);

    FileTokenIngestor ingestor = new FileTokenIngestor(tokenFile);
    Optional<UserContext> result = ingestor.load();
    assertTrue(result.isPresent());
    assertEquals("user@example.com", result.get().getPrincipal());
    assertEquals("https://accounts.google.com", result.get().getIssuer());
  }
}
