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

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * A fake credential provider for testing. Supports schemes "fake" and "shared".
 */
public class FakeCredentialProvider implements CredentialProvider {

  /** Sentinel URI host that triggers a CredentialResolutionException. */
  public static final String ERROR_HOST = "error.example.com";

  private Map<String, String> initConf;
  private int initCount;

  @Override
  public void init(Map<String, String> conf) {
    this.initConf = conf;
    this.initCount++;
  }

  @Override
  public Set<String> supportedSchemes() {
    return Set.of("fake", "shared");
  }

  @Override
  public ServiceCredential resolve(UserContext user, URI target)
      throws CredentialResolutionException {
    if (target.getHost() != null && target.getHost().equals(ERROR_HOST)) {
      throw new CredentialResolutionException(
          "Simulated failure for target: " + target);
    }
    Instant expiresAt = Instant.now().plus(suggestedTtl());
    return new ServiceCredential(Map.of("provider", "fake"), expiresAt);
  }

  /** Returns the configuration map passed to {@link #init(Map)}, or null if not yet called. */
  public Map<String, String> getInitConf() {
    return initConf;
  }

  /** Returns the number of times {@link #init(Map)} has been called. */
  public int getInitCount() {
    return initCount;
  }
}
