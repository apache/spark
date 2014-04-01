/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.authorize;

import java.security.Policy;

/**
 * {@link PolicyProvider} provides the {@link Service} definitions to the
 * security {@link Policy} in effect for Hadoop.
 *
 */
public abstract class PolicyProvider {

  /**
   * Configuration key for the {@link PolicyProvider} implementation.
   */
  public static final String POLICY_PROVIDER_CONFIG = 
    "hadoop.security.authorization.policyprovider";
  
  /**
   * A default {@link PolicyProvider} without any defined services.
   */
  public static final PolicyProvider DEFAULT_POLICY_PROVIDER =
    new PolicyProvider() {
    public Service[] getServices() {
      return null;
    }
  };
  
  /**
   * Get the {@link Service} definitions from the {@link PolicyProvider}.
   * @return the {@link Service} definitions
   */
  public abstract Service[] getServices();
}
