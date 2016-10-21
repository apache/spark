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

package org.apache.spark.scheduler

import scala.util.Failure

import org.apache.spark.SparkFunSuite

class JobWaiterSuite extends SparkFunSuite {

  test("call jobFailed multiple times") {
    val waiter = new JobWaiter[Int](null, 0, totalTasks = 2, null)

    // Should not throw exception if calling jobFailed multiple times
    waiter.jobFailed(new RuntimeException("Oops 1"))
    waiter.jobFailed(new RuntimeException("Oops 2"))
    waiter.jobFailed(new RuntimeException("Oops 3"))

    waiter.completionFuture.value match {
      case Some(Failure(e)) =>
        // We should receive the first exception
        assert("Oops 1" === e.getMessage)
      case other => fail("Should receiver the first exception but it was " + other)
    }
  }
}
