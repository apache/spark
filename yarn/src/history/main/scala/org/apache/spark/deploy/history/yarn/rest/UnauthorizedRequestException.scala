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
package org.apache.spark.deploy.history.yarn.rest

import java.io.IOException

/**
 * Special exception to indicate a request was forbidden/unauthorized.
 * This implies that it's potentially a token-renewal-related failure.
 * @param msg text to use in the message
 * @param cause optional cause
 */
private[spark] class UnauthorizedRequestException(
    url: String,
    private val msg: String, private val cause: Throwable = null)
    extends IOException(msg, cause) {
}
