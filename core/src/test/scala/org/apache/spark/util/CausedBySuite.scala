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

package org.apache.spark.util

import org.apache.spark.SparkFunSuite

class CausedBySuite extends SparkFunSuite {

  test("For an error without a cause, should return the error") {
    val error = new Exception

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === error)
  }

  test("For an error with a cause, should return the cause of the error") {
    val cause = new Exception
    val error = new Exception(cause)

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === cause)
  }

  test("For an error with a cause that itself has a cause, return the root cause") {
    val causeOfCause = new Exception
    val cause = new Exception(causeOfCause)
    val error = new Exception(cause)

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === causeOfCause)
  }
}
