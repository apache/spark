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
import org.apache.spark.streaming.util.MasterFailureTest
import StreamingContext._

import org.scalatest.{FunSuite, BeforeAndAfter}
import com.google.common.io.Files
import java.io.File
import org.apache.commons.io.FileUtils
import collection.mutable.ArrayBuffer


/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends FunSuite with BeforeAndAfter with Logging {

  var directory = "FailureSuite"
  val numBatches = 30
  val batchDuration = Milliseconds(1000)

  before {
    FileUtils.deleteDirectory(new File(directory))
  }

  after {
    FileUtils.deleteDirectory(new File(directory))
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(directory, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory, numBatches, batchDuration)
  }
}

