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

package org.apache.spark.sql.execution.streaming

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.streaming.StreamTest

class ConsoleSinkSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("SPARK-16020 Complete mode aggregation with console sink") {
    withTempDir { checkpointLocation =>
      val origOut = System.out
      val stdout = new ByteArrayOutputStream()
      try {
        // Hook Java System.out.println
        System.setOut(new PrintStream(stdout))
        // Hook Scala println
        Console.withOut(stdout) {
          val input = MemoryStream[String]
          val df = input.toDF().groupBy("value").count()
          val query = df.writeStream
            .format("console")
            .outputMode("complete")
            .option("checkpointLocation", checkpointLocation.getAbsolutePath)
            .start()
          input.addData("a")
          query.processAllAvailable()
          input.addData("a", "b")
          query.processAllAvailable()
          input.addData("a", "b", "c")
          query.processAllAvailable()
          query.stop()
        }
        System.out.flush()
      } finally {
        System.setOut(origOut)
      }

      val expected = """-------------------------------------------
        |Batch: 0
        |-------------------------------------------
        |+-----+-----+
        ||value|count|
        |+-----+-----+
        ||    a|    1|
        |+-----+-----+
        |
        |-------------------------------------------
        |Batch: 1
        |-------------------------------------------
        |+-----+-----+
        ||value|count|
        |+-----+-----+
        ||    a|    2|
        ||    b|    1|
        |+-----+-----+
        |
        |-------------------------------------------
        |Batch: 2
        |-------------------------------------------
        |+-----+-----+
        ||value|count|
        |+-----+-----+
        ||    a|    3|
        ||    b|    2|
        ||    c|    1|
        |+-----+-----+
        |
        |""".stripMargin
      assert(expected === new String(stdout.toByteArray, UTF_8))
    }
  }

}
