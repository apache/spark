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

package org.apache.spark.security.aws;

import java.util.Map;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.apache.spark.SparkEnv;
import org.apache.spark.VersionedCredentials;
import org.apache.spark.deploy.security.UserCredentialManager;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserCredentials;

/**
 * A dynamic AWS credentials provider for executor-side S3A access that reads from
 * Spark's credential store.
 * <p>
 * This provider uses version-based caching: credentials are deserialized only when
 * the credential store version changes (i.e., after a driver-initiated refresh).
 * Since the store version is monotonically increasing and only changes on renewal
 * (minutes-scale), the cache hit rate is {@literal >}99.99% for I/O-heavy workloads.
 * <p>
 * This implementation is thread-safe. Multiple threads may call
 * {@code resolveCredentials()} concurrently without external synchronization.
 * Each invocation reads the credential version atomically and returns either
 * the cached result or a freshly deserialized one.
 * <p>
 * Configure via:
 * {@code fs.s3a.aws.credentials.provider=
 * org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider}
 *
 * @since 4.4.0
 */
public class SparkOidcAwsCredentialsProvider implements AwsCredentialsProvider {

  /** S3A credential property keys (same as produced by AwsStsCredentialProvider). */
  private static final String ACCESS_KEY = "fs.s3a.access.key";
  private static final String SECRET_KEY = "fs.s3a.secret.key";
  private static final String SESSION_TOKEN = "fs.s3a.session.token";

  /** The S3A scheme used to look up credentials in the UserCredentials bundle. */
  private static final String S3A_SCHEME = "s3a";

  /** Version-keyed cache to avoid repeated deserialization on every S3A API call. */
  private volatile CachedResult cached;

  private record CachedResult(long version, AwsSessionCredentials credentials) {}

  @Override
  public AwsCredentials resolveCredentials() {
    SparkEnv env = SparkEnv.get();
    if (env == null) {
      throw new IllegalStateException(
          "SparkEnv is not available. SparkOidcAwsCredentialsProvider can only be used "
              + "within an active Spark executor.");
    }

    VersionedCredentials versioned = env.userCredentials().get();
    if (versioned == null) {
      throw new IllegalStateException(
          "No credentials available in the executor credential store. "
              + "Ensure spark.security.oidc.enabled=true and the driver has acquired "
              + "credentials before executor tasks run.");
    }

    // Fast path: return cached credentials if version hasn't changed.
    CachedResult current = cached;
    if (current != null && current.version() == versioned.version()) {
      return current.credentials();
    }

    UserCredentials credentials;
    try {
      credentials = UserCredentialManager.deserializeUserCredentials(versioned.bytes());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to deserialize credentials from executor store (version="
              + versioned.version() + "). The credential bytes may be corrupted or "
              + "incompatible with this Spark version.", e);
    }

    ServiceCredential s3aCred = credentials.forScheme(S3A_SCHEME).orElse(null);
    if (s3aCred == null) {
      throw new IllegalStateException(
          "No credential found for scheme '" + S3A_SCHEME + "' in the executor "
              + "credential store. Ensure an S3A-compatible CredentialProvider "
              + "(e.g., AwsStsCredentialProvider) is configured on the driver.");
    }

    Map<String, String> props = s3aCred.getProperties();
    String accessKey = props.get(ACCESS_KEY);
    String secretKey = props.get(SECRET_KEY);
    String sessionToken = props.get(SESSION_TOKEN);

    if (accessKey == null || accessKey.isEmpty()
        || secretKey == null || secretKey.isEmpty()
        || sessionToken == null || sessionToken.isEmpty()) {
      throw new IllegalStateException(
          "ServiceCredential for scheme '" + S3A_SCHEME + "' is missing required "
              + "properties. Expected non-empty values for: " + ACCESS_KEY + ", "
              + SECRET_KEY + ", " + SESSION_TOKEN);
    }

    AwsSessionCredentials result = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
    cached = new CachedResult(versioned.version(), result);
    return result;
  }
}
