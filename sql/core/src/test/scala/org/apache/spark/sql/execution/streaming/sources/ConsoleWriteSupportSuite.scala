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

package org.apache.spark.sql.execution.streaming.sources

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamTest, Trigger}

class ConsoleWriteSupportSuite extends StreamTest {
  import testImplicits._

  test("microbatch - default") {
    val input = MemoryStream[Int]

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      val query = input.toDF().writeStream.format("console").start()
      try {
        input.addData(1, 2, 3)
        query.processAllAvailable()
        input.addData(4, 5, 6)
        query.processAllAvailable()
        input.addData()
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }

    assert(captured.toString() ==
      """-------------------------------------------
        |Batch: 0
        |-------------------------------------------
        |+-----+
        ||value|
        |+-----+
        ||    1|
        ||    2|
        ||    3|
        |+-----+
        |
        |-------------------------------------------
        |Batch: 1
        |-------------------------------------------
        |+-----+
        ||value|
        |+-----+
        ||    4|
        ||    5|
        ||    6|
        |+-----+
        |
        |-------------------------------------------
        |Batch: 2
        |-------------------------------------------
        |+-----+
        ||value|
        |+-----+
        |+-----+
        |
        |""".stripMargin)
  }

  test("microbatch - with numRows") {
    val input = MemoryStream[Int]

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      val query = input.toDF().writeStream.format("console").option("NUMROWS", 2).start()
      try {
        input.addData(1, 2, 3)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }

    assert(captured.toString() ==
      """-------------------------------------------
        |Batch: 0
        |-------------------------------------------
        |+-----+
        ||value|
        |+-----+
        ||    1|
        ||    2|
        |+-----+
        |only showing top 2 rows
        |
        |""".stripMargin)
  }

  test("microbatch - truncation") {
    val input = MemoryStream[String]

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      val query = input.toDF().writeStream.format("console").option("TRUNCATE", true).start()
      try {
        input.addData("123456789012345678901234567890")
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }

    assert(captured.toString() ==
      """-------------------------------------------
        |Batch: 0
        |-------------------------------------------
        |+--------------------+
        ||               value|
        |+--------------------+
        ||12345678901234567...|
        |+--------------------+
        |
        |""".stripMargin)
  }

  test("continuous - default") {
    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      val input = spark.readStream
        .format("rate")
        .option("numPartitions", "1")
        .option("rowsPerSecond", "5")
        .load()
        .select(Symbol("value"))

      val query = input.writeStream.format("console").trigger(Trigger.Continuous(200)).start()
      assert(query.isActive)
      query.stop()
    }
  }
}
