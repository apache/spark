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
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.deploy.security.UserCredentialManager;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserCredentials;

/**
 * :: DeveloperApi ::
 * A dynamic AWS credentials provider for executor-side S3A access that reads from
 * Spark's credential store on every {@code resolveCredentials()} call.
 * <p>
 * This provider never caches credentials internally. Each call reads the latest
 * {@link ServiceCredential} from the executor's {@code SparkEnv.userCredentials} store,
 * ensuring that credential refreshes (delivered via {@code UpdateUserCredentials} RPC or
 * {@code TaskDescription}) are immediately visible to S3A without requiring FileSystem
 * cache invalidation.
 * <p>
 * Configure via:
 * {@code fs.s3a.aws.credentials.provider=org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider}
 * <p>
 * When {@code spark.security.oidc.enabled=true} and the user has not explicitly set
 * {@code fs.s3a.aws.credentials.provider}, this provider is auto-configured.
 *
 * @since 5.0.0
 */
@DeveloperApi
public class SparkOidcAwsCredentialsProvider implements AwsCredentialsProvider {

  /** S3A credential property keys (same as produced by AwsStsCredentialProvider). */
  private static final String ACCESS_KEY = "fs.s3a.access.key";
  private static final String SECRET_KEY = "fs.s3a.secret.key";
  private static final String SESSION_TOKEN = "fs.s3a.session.token";

  /** The S3A scheme used to look up credentials in the UserCredentials bundle. */
  private static final String S3A_SCHEME = "s3a";

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

    if (accessKey == null || secretKey == null || sessionToken == null) {
      throw new IllegalStateException(
          "ServiceCredential for scheme '" + S3A_SCHEME + "' is missing required "
              + "properties. Expected: " + ACCESS_KEY + ", " + SECRET_KEY + ", "
              + SESSION_TOKEN);
    }

    return AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
  }
}
