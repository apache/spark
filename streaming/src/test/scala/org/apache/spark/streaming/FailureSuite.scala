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

package org.apache.spark.streaming

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends TestSuiteBase with Logging {

  val directory = Utils.createTempDir()
  val numBatches = 30

  override def batchDuration: Duration = Milliseconds(1000)

  override def useManualClock: Boolean = false

  override def afterFunction() {
    Utils.deleteRecursively(directory)
    super.afterFunction()
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(directory.getAbsolutePath, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory.getAbsolutePath, numBatches, batchDuration)
  }
}

