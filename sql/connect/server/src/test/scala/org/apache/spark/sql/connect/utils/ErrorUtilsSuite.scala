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
package org.apache.spark.sql.connect.utils

import java.util.UUID

import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver

import org.apache.spark.sql.test.SharedSparkSession

class ErrorUtilsSuite extends SharedSparkSession {

  test("handleError fallback uses throwable class when fatal throwable has no message") {
    val observer = new StreamObserver[Unit] {
      override def onNext(value: Unit): Unit = {
        fail(s"Unexpected response: $value")
      }

      override def onError(t: Throwable): Unit = {
        throw t
      }

      override def onCompleted(): Unit = {
        fail("Unexpected completion")
      }
    }

    val error = intercept[StatusRuntimeException] {
      ErrorUtils.handleError("execute", observer, "user1", UUID.randomUUID().toString)(
        new InterruptedException())
    }

    assert(error.getStatus.getCode == Status.Code.UNKNOWN)
    assert(error.getStatus.getDescription == classOf[InterruptedException].getName)
  }
}
