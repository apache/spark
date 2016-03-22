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
import java.util.UUID

import org.apache.spark.sql.{ContinuousQueryException, ContinuousQuery, StreamTest}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

import scala.util.Random
import scala.util.control.NonFatal

class FileStressSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("fault tolerance stress test") {
    val numRecords = 1000000
    val inputDir = Utils.createTempDir("stream.input").getCanonicalPath
    val stagingDir = Utils.createTempDir("stream.staging").getCanonicalPath
    val outputDir = Utils.createTempDir("stream.output").getCanonicalPath
    val checkpoint = Utils.createTempDir("stream.checkpoint").getCanonicalPath

    @volatile
    var continue = true
    var stream: ContinuousQuery = null

    val writer = new Thread("stream writer") {
      override def run(): Unit = {
        var i = numRecords
        while (i > 0) {
          val count = Random.nextInt(100)
          var j = 0
          var string = ""
          while (j < count && i > 0) {
            string = string + i + "\n"
            j += 1
            i -= 1
          }

          val uuid = UUID.randomUUID().toString
          val fileName = new File(stagingDir, uuid)
          stringToFile(fileName, string)
          fileName.renameTo(new File(inputDir, uuid))
          val sleep = Random.nextInt(100)
          Thread.sleep(sleep)
        }

        println("DONE WRITING")
        var done = false
        while (!done) {
          try {
            stream.processAllAvailable()
            done = true
          } catch {
            case NonFatal(_) =>
          }
        }

        println("STOPPING QUERY")
        continue = false
        stream.stop()
      }
    }
    writer.start()

    val input = sqlContext.read.format("text").stream(inputDir)
    def startStream(): ContinuousQuery = input
        .repartition(5)
        .as[String]
        .mapPartitions { iter =>
          val rand = Random.nextInt(100)
          if (rand < 5) { sys.error("failure") }
          iter.map(_.toLong)
        }
        .write
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .startStream(outputDir)

    var failures = 0
    val streamThread = new Thread("stream runner") {
      while (continue) {
        println("Starting stream")
        stream = startStream()

        try {
          stream.awaitTermination()
        } catch {
          case ce: ContinuousQueryException =>
            failures += 1
        }
      }
    }

    streamThread.join()

    println(s"Stream restarted $failures times.")
    assert(sqlContext.read.parquet(outputDir).distinct().count() == numRecords)
  }
}

class FileStreamSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("unpartitioned writing") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir("stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir("stream.checkpoint").getCanonicalPath

    val query =
      df.write
        .format("parquet")
        .option("checkpointLocation", checkpointDir)
        .startStream(outputDir)

    inputData.addData(1, 2, 3)
    println("blocking")
    failAfter(streamingTimeout) { query.processAllAvailable() }
    println("done")

    val outputDf = sqlContext.read.parquet(outputDir).as[Int]

    checkDataset(
      outputDf,
      1, 2, 3)
  }
}