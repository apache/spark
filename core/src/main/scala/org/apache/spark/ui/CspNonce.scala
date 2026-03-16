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

package org.apache.spark.ui

import java.util.UUID

/**
 * Thread-local storage for CSP nonce values.
 *
 * A nonce is generated per request in [[HttpSecurityFilter]] and used by UI pages
 * to mark inline scripts and styles as trusted.
 */
private[spark] object CspNonce {

  private val nonce = new ThreadLocal[String]

  /** Generate a new nonce and store it in the current thread. */
  def generate(): String = {
    val value = UUID.randomUUID().toString
    nonce.set(value)
    value
  }

  /** Get the nonce for the current request. */
  def get: String = nonce.get()

  /** Remove the nonce after the request is complete. */
  def clear(): Unit = nonce.remove()
}
