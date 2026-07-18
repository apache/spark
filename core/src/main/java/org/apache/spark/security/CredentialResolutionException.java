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

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Thrown when a {@link CredentialProvider} fails to resolve credentials for a target URI.
 * <p>
 * This is a checked exception to ensure callers handle credential resolution failures
 * explicitly (e.g., retry, fail the job, or fall back to another mechanism).
 *
 * @since 4.3.0
 */
@DeveloperApi
public class CredentialResolutionException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public CredentialResolutionException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the underlying cause
   */
  public CredentialResolutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
