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

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.util.Utils

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends SparkFunSuite with BeforeAndAfter {

  private val batchDuration: Duration = Milliseconds(1000)
  private val numBatches = 30
  private var directory: File = null

  before {
    directory = Utils.createTempDir()
  }

  after {
    if (directory != null) {
      Utils.deleteRecursively(directory)
    }
    StreamingContext.getActive().foreach { _.stop() }

    // Stop SparkContext if active
    SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("bla")).stop()
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(directory.getAbsolutePath, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory.getAbsolutePath, numBatches, batchDuration)
  }
}

