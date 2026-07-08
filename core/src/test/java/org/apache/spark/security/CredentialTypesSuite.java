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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CredentialTypesSuite {

  private static final String TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.secret";
  private static final Instant NOW = Instant.parse("2026-01-15T12:00:00Z");
  private static final Instant PAST = Instant.parse("2026-01-15T11:00:00Z");
  private static final Instant FUTURE = Instant.parse("2026-01-15T13:00:00Z");

  // --- UserContext tests ---

  @Test
  public void testUserContextGettersRoundTrip() {
    UserContext ctx = new UserContext("user1", "https://idp.example.com", TOKEN, NOW, FUTURE);
    assertEquals("user1", ctx.getPrincipal());
    assertEquals("https://idp.example.com", ctx.getIssuer());
    assertEquals(TOKEN, ctx.getRawToken());
    assertEquals(NOW, ctx.getIssuedAt());
    assertEquals(FUTURE, ctx.getExpiresAt());
  }

  @Test
  public void testUserContextToStringRedactsToken() {
    UserContext ctx = new UserContext("user1", "https://idp.example.com", TOKEN, NOW, FUTURE);
    String str = ctx.toString();
    assertFalse(str.contains(TOKEN), "toString must not contain the raw token value");
    assertTrue(str.contains("[REDACTED]"), "toString must contain [REDACTED]");
    assertTrue(str.contains("user1"));
    assertTrue(str.contains("https://idp.example.com"));
  }

  @Test
  public void testUserContextIsExpiredBoundary() {
    // expiresAt == NOW -> expired (now >= expiresAt)
    UserContext ctx = new UserContext("u", "iss", TOKEN, PAST, NOW);
    assertTrue(ctx.isExpired(NOW), "Should be expired when now == expiresAt");
    assertFalse(ctx.isExpired(PAST), "Should not be expired when now < expiresAt");
    assertTrue(ctx.isExpired(FUTURE), "Should be expired when now > expiresAt");
  }

  @Test
  public void testUserContextIsExpiredWithNullExpiry() {
    UserContext ctx = new UserContext("u", "iss", TOKEN, NOW, null);
    assertFalse(ctx.isExpired(NOW), "Should not be expired when expiresAt is null");
  }

  @Test
  public void testUserContextEqualsAndHashCode() {
    UserContext a = new UserContext("u", "iss", TOKEN, NOW, FUTURE);
    UserContext b = new UserContext("u", "iss", TOKEN, NOW, FUTURE);
    UserContext c = new UserContext("other", "iss", TOKEN, NOW, FUTURE);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  public void testUserContextRawTokenExcludedFromJackson() throws Exception {
    String sentinel = "SENTINEL_SECRET_JWT_abc123";
    UserContext ctx = new UserContext("alice", "https://idp.example.com", sentinel, NOW, FUTURE);

    ObjectMapper mapper = JsonMapper.builder()
        .disable(MapperFeature.REQUIRE_HANDLERS_FOR_JAVA8_TIMES)
        .build();

    String json = mapper.writeValueAsString(ctx);

    assertFalse(json.contains(sentinel),
        "Jackson serialization must not contain the raw token value");
    assertTrue(json.contains("alice"),
        "Jackson serialization must contain the principal to prove serialization occurred");
    assertTrue(json.contains("https://idp.example.com"),
        "Jackson serialization must contain the issuer");
    assertFalse(json.contains("rawToken"),
        "Jackson serialization must not contain a rawToken field");
  }

  @Test
  public void testUserContextNotSerializable() {
    UserContext ctx = new UserContext("user1", "https://idp.example.com", TOKEN, NOW, FUTURE);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    assertThrows(NotSerializableException.class, () -> {
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(ctx);
      }
    });
  }

  // --- ServiceCredential tests ---

  @Test
  public void testServiceCredentialDefensiveCopy() {
    Map<String, String> props = new HashMap<>();
    props.put("key", "value");
    ServiceCredential cred = new ServiceCredential(props, FUTURE);
    // Mutating the original map must not affect the credential
    props.put("key2", "value2");
    assertFalse(cred.getProperties().containsKey("key2"));
    assertEquals(1, cred.getProperties().size());
  }

  @Test
  public void testServiceCredentialIsExpired() {
    ServiceCredential cred = new ServiceCredential(Map.of("k", "v"), NOW);
    assertTrue(cred.isExpired(NOW), "Should be expired when now == expiresAt");
    assertFalse(cred.isExpired(PAST), "Should not be expired when now < expiresAt");
    assertTrue(cred.isExpired(FUTURE), "Should be expired when now > expiresAt");
  }

  @Test
  public void testServiceCredentialIsExpiredWithNullExpiry() {
    ServiceCredential cred = new ServiceCredential(Map.of("k", "v"), null);
    assertFalse(cred.isExpired(NOW));
  }

  @Test
  public void testServiceCredentialSerializationRoundTrip() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE");
    props.put("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE");
    ServiceCredential original = new ServiceCredential(props, FUTURE);

    byte[] bytes = serialize(original);
    ServiceCredential deserialized = (ServiceCredential) deserialize(bytes);

    assertEquals(original, deserialized);
    assertEquals(original.getProperties(), deserialized.getProperties());
    assertEquals(original.getExpiresAt(), deserialized.getExpiresAt());
  }

  @Test
  public void testServiceCredentialEqualsAndHashCode() {
    ServiceCredential a = new ServiceCredential(Map.of("k", "v"), FUTURE);
    ServiceCredential b = new ServiceCredential(Map.of("k", "v"), FUTURE);
    ServiceCredential c = new ServiceCredential(Map.of("k", "other"), FUTURE);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  public void testServiceCredentialToStringRedactsValues() {
    Map<String, String> props = new HashMap<>();
    props.put("accessKey", "AKIAIOSFODNN7EXAMPLE");
    props.put("secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE");
    ServiceCredential cred = new ServiceCredential(props, FUTURE);
    String str = cred.toString();
    // Secret values must NOT appear
    assertFalse(str.contains("AKIAIOSFODNN7EXAMPLE"),
        "toString must not contain secret property values");
    assertFalse(str.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE"),
        "toString must not contain secret property values");
    // Keys SHOULD appear for debuggability
    assertTrue(str.contains("accessKey"), "toString should contain property key names");
    assertTrue(str.contains("secretKey"), "toString should contain property key names");
    // Values are replaced with [REDACTED]
    assertTrue(str.contains("[REDACTED]"), "toString must contain [REDACTED] for values");
    // expiresAt should still appear
    assertTrue(str.contains("expiresAt"), "toString should contain expiresAt");
  }

  @Test
  public void testServiceCredentialToStringEmptyProperties() {
    ServiceCredential cred = new ServiceCredential(Map.of(), null);
    String str = cred.toString();
    assertTrue(str.contains("properties={}"),
        "toString should handle empty properties map");
    assertTrue(str.contains("expiresAt=null"));
  }

  // --- UserCredentials tests ---

  @Test
  public void testUserCredentialsForSchemePresent() {
    ServiceCredential s3Cred = new ServiceCredential(Map.of("k", "v"), FUTURE);
    UserCredentials uc = new UserCredentials(Map.of("s3a", s3Cred));
    Optional<ServiceCredential> result = uc.forScheme("s3a");
    assertTrue(result.isPresent());
    assertEquals(s3Cred, result.get());
  }

  @Test
  public void testUserCredentialsForSchemeAbsent() {
    ServiceCredential s3Cred = new ServiceCredential(Map.of("k", "v"), FUTURE);
    UserCredentials uc = new UserCredentials(Map.of("s3a", s3Cred));
    Optional<ServiceCredential> result = uc.forScheme("abfss");
    assertFalse(result.isPresent());
  }

  @Test
  public void testUserCredentialsDoesNotExposeToken() {
    // UserCredentials only contains ServiceCredentials, not UserContext or tokens
    ServiceCredential cred = new ServiceCredential(Map.of("token", "short-lived"), FUTURE);
    UserCredentials uc = new UserCredentials(Map.of("s3a", cred));
    String str = uc.toString();
    assertFalse(str.contains(TOKEN), "UserCredentials must not contain raw identity token");
    // ServiceCredential values should also be redacted in nested toString
    assertFalse(str.contains("short-lived"),
        "UserCredentials must not expose ServiceCredential property values");
  }

  @Test
  public void testUserCredentialsSerializationRoundTrip() throws Exception {
    Map<String, ServiceCredential> creds = new HashMap<>();
    creds.put("s3a", new ServiceCredential(Map.of("key", "val1"), FUTURE));
    creds.put("abfss", new ServiceCredential(Map.of("token", "val2"), NOW));
    UserCredentials original = new UserCredentials(creds);

    byte[] bytes = serialize(original);
    UserCredentials deserialized = (UserCredentials) deserialize(bytes);

    assertEquals(original, deserialized);
    assertEquals(original.getCredentials(), deserialized.getCredentials());
  }

  @Test
  public void testUserCredentialsDefensiveCopy() {
    Map<String, ServiceCredential> creds = new HashMap<>();
    creds.put("s3a", new ServiceCredential(Map.of("k", "v"), FUTURE));
    UserCredentials uc = new UserCredentials(creds);
    // Mutating the original map must not affect the credentials
    creds.put("extra", new ServiceCredential(Map.of("x", "y"), NOW));
    assertFalse(uc.forScheme("extra").isPresent());
  }

  @Test
  public void testUserCredentialsEqualsAndHashCode() {
    ServiceCredential cred = new ServiceCredential(Map.of("k", "v"), FUTURE);
    UserCredentials a = new UserCredentials(Map.of("s3a", cred));
    UserCredentials b = new UserCredentials(Map.of("s3a", cred));
    UserCredentials c = new UserCredentials(Map.of("abfss", cred));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  public void testUserCredentialsForSchemeCaseInsensitive() {
    ServiceCredential cred = new ServiceCredential(Map.of("key", "val"), FUTURE);
    // Store with lowercase key, lookup with uppercase
    UserCredentials uc1 = new UserCredentials(Map.of("s3a", cred));
    assertTrue(uc1.forScheme("S3A").isPresent(), "Uppercase lookup should find lowercase key");
    assertEquals(cred, uc1.forScheme("S3A").get());
    assertTrue(uc1.forScheme("s3a").isPresent(), "Exact case lookup should still work");
    assertTrue(uc1.forScheme("S3a").isPresent(), "Mixed case lookup should work");

    // Store with uppercase key, lookup with lowercase
    UserCredentials uc2 = new UserCredentials(Map.of("ABFSS", cred));
    assertTrue(uc2.forScheme("abfss").isPresent(), "Lowercase lookup should find uppercase key");
    assertEquals(cred, uc2.forScheme("abfss").get());

    // Absent scheme still returns empty
    assertFalse(uc2.forScheme("hdfs").isPresent(), "Absent scheme should return empty");
  }

  // --- Helpers ---

  private static byte[] serialize(Object obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    return baos.toByteArray();
  }

  private static Object deserialize(byte[] bytes) throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    }
  }
}
