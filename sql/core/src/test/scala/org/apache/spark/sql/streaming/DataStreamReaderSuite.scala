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

import java.io.File

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class DataStreamReaderSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("read from text files") {
    val src = Utils.createDirectory("streaming.src")
    val dest = Utils.createDirectory("streaming.dest")

    val df =
      sqlContext
        .streamFrom
        .format("text")
        .open(src.getCanonicalPath)

    val filtered = df.filter($"value" contains "keep")

    val runningQuery =
      filtered
        .streamTo
        .format("text")
        .start(dest.getCanonicalPath)

    runningQuery.clearBatchMarker()

    // Add some data
    stringToFile(new File(src, "1"), "drop1\nkeep2\nkeep3")

    runningQuery.awaitBatchCompletion()

    val output = sqlContext.read.text(dest.getCanonicalPath).as[String]
    checkAnswer(output, "keep2", "keep3")

    runningQuery.stop()
  }
}
