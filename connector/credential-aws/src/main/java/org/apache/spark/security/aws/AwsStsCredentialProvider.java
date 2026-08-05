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

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.security.CredentialProvider;
import org.apache.spark.security.CredentialResolutionException;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserContext;

/**
 * :: DeveloperApi ::
 * AWS STS credential provider that exchanges an OIDC identity token for temporary
 * AWS credentials via {@code AssumeRoleWithWebIdentity}.
 *
 * @since 4.3.0
 */
@DeveloperApi
public class AwsStsCredentialProvider implements CredentialProvider {

  @Override
  public void init(Map<String, String> conf) {
    throw new UnsupportedOperationException(
        "AwsStsCredentialProvider is a stub. Full implementation in SPARK-57898.");
  }

  @Override
  public Set<String> supportedSchemes() {
    return Set.of("s3a");
  }

  @Override
  public ServiceCredential resolve(UserContext user, URI target)
      throws CredentialResolutionException {
    throw new UnsupportedOperationException(
        "AwsStsCredentialProvider is a stub. Full implementation in SPARK-57898.");
  }
}
