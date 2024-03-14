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

package org.apache.spark.api.python

import java.net.SocketTimeoutException

// scalastyle:off executioncontextglobal
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:on executioncontextglobal
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.spark.SharedSparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ThreadUtils

// Tests for PythonWorkerFactory.
class PythonWorkerFactorySuite extends SparkFunSuite with SharedSparkContext {

  test("createSimpleWorker() fails with a timeout error if worker does not connect back") {
    // It verifies that server side times out in accept(), if the worker does not connect back.
    // E.g. the worker might fail at the beginning before it tries to connect back.

    val workerFactory = new PythonWorkerFactory(
      "python3", "pyspark.testing.non_existing_worker_module", Map.empty, false
    )

    // Create the worker in a separate thread so that if there is a bug where it does not
    // return (accept() used to be blocking), the test doesn't hang for a long time.
    val createFuture = Future {
      val ex = intercept[SparkException] {
        workerFactory.createSimpleWorker(blockingMode = true) // blockingMode doesn't matter.
        // NOTE: This takes 10 seconds (which is the accept timeout in PythonWorkerFactory).
        // That makes this a bit longish test.
      }
      assert(ex.getMessage.contains("Python worker failed to connect back"))
      assert(ex.getCause.isInstanceOf[SocketTimeoutException])
    }

    // Timeout ensures that the test fails in 5 minutes if createSimplerWorker() doesn't return.
    ThreadUtils.awaitReady(createFuture, 5.minutes)
  }
}
