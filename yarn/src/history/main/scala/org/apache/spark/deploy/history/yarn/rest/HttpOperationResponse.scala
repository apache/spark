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

/**
 * A response for use as a return value from operations
 */
private[spark] class HttpOperationResponse {
  /** HTTP response code */
  var responseCode: Int = 0

  /** last modified timestamp */
  var lastModified: Long = 0L

  /** Content-type value */
  var contentType: String = ""

  /** reponse message text */
  var responseMessage: String = ""

  /** response body as byte array */
  var data: Array[Byte] = new Array(0)

  override def toString: String = {
    s"$responseCode $responseMessage; last modified $lastModified," +
      s" contentType $contentType" +
      (if (data == null) "" else s"data[${data.length}]")
  }

  /**
   * Calculate the current response line
   * @return an HTTP response ilne
   */
  def responseLine: String = s"$responseCode $responseMessage"

  /**
   * Convert the response data into a string and return it.
   *
   * There must be some response data for this to work
   * @return the body as a string.
   */
  def responseBody: String = {
    require(data != null, s"no response body in $this")
    new String(data)
  }

}
