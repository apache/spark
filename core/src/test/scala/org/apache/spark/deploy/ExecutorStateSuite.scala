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

package org.apache.spark.deploy

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.ExecutorState._

class ExecutorStateSuite extends SparkFunSuite with Matchers {

  test("Executor State should be finished if it is killed, failed, lost or exited") {
    Seq(KILLED, FAILED, LOST, EXITED)
      .foreach(executorState => isFinished(executorState) should be(true))
  }

  test("Executor State should not be finished if it is running or launching") {
    isFinished(LAUNCHING) should be (false)
    isFinished(RUNNING) should be (false)
  }

}
