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

import org.apache.spark.sql.{AnalysisException, Row, StreamTest}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class MemorySinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("registering as a table") {
    testRegisterAsTable()
  }

  ignore("stress test") {
    // Ignore the stress test as it takes several minutes to run
    (0 until 1000).foreach(_ => testRegisterAsTable())
  }

  private def testRegisterAsTable(): Unit = {
    val input = MemoryStream[Int]
    val query = input.toDF().write
      .format("memory")
      .queryName("memStream")
      .startStream()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3)

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3, 4, 5, 6)

    query.stop()
  }

  test("error when no name is specified") {
    val error = intercept[AnalysisException] {
      val input = MemoryStream[Int]
      val query = input.toDF().write
          .format("memory")
          .startStream()
    }

    assert(error.message contains "queryName must be specified")
  }

  test("error if attempting to resume specific checkpoint") {
    val location = Utils.createTempDir(namePrefix = "steaming.checkpoint").getCanonicalPath

    val input = MemoryStream[Int]
    val query = input.toDF().write
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .startStream()
    input.addData(1, 2, 3)
    query.processAllAvailable()
    query.stop()

    intercept[AnalysisException] {
      input.toDF().write
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .startStream()
    }
  }
}
