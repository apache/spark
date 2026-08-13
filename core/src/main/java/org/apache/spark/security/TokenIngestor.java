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

import java.util.Optional;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Read an OIDC identity token and produces a {@link UserContext}.
 * <p>
 * Implementation should be stateless with respect to Spark configuration;
 * configuration is passed at construction time.
 *
 * @since 4.3.0
 */
@DeveloperApi
public interface TokenIngestor {

  /**
   * Attempt to load the current identity token and parse it into a UserContext.
   *
   * @return a present Optional containing the UserContext if a valid token is available,
   * or empty if unavailable (e.g. empty content / missing file).
   */
  Optional<UserContext> load();
}
