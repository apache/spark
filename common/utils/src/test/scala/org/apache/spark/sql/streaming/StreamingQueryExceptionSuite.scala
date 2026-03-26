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

package org.apache.spark.sql.streaming

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class StreamingQueryExceptionSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("toString with null cause should not throw NPE") {
    val exception = new StreamingQueryException(
      message = "test message",
      cause = null,
      errorClass = "TEST_ERROR",
      messageParameters = Map.empty[String, String])

    val result = exception.toString()
    assert(result.contains("StreamingQueryException"))
    assert(result.contains("test message"))
    assert(result.contains("(no cause)"))
  }

  test("toString with non-null cause should use cause message") {
    val cause = new RuntimeException("root cause")
    val exception = new StreamingQueryException(
      message = "test message",
      cause = cause,
      errorClass = "TEST_ERROR",
      messageParameters = Map.empty[String, String])

    val result = exception.toString()
    assert(result.contains("StreamingQueryException"))
    assert(result.contains("root cause"))
  }

  test("toString with non-null cause but null cause message") {
    val cause = new RuntimeException()
    val exception = new StreamingQueryException(
      message = "test message",
      cause = cause,
      errorClass = "TEST_ERROR",
      messageParameters = Map.empty[String, String])

    val result = exception.toString()
    assert(result.contains("StreamingQueryException"))
    assert(result.contains("test message"))
    assert(result.contains("(no cause message)"))
  }
}
