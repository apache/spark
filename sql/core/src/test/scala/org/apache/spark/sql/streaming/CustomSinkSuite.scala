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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CustomSinkSuite extends StreamTest {
  import testImplicits._

  /**
   * Creates a custom sink similar to the old foreachRDD. Provided function is called for each
   * time slice with the dataset representing the time slice.
   * Provided func must consume the dataset (e.g. call `foreach` or `collect`).
   * As per SPARK-16020 arbitrary transformations are not supported, but converting to an RDD
   * will allow for more transformations beyond `foreach` and `collect` while preserving the
   * incremental planning.
   */
  class ForeachDatasetSinkProvider(func: DataFrame => Unit) extends StreamSinkProvider {
    def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): ForeachDatasetSink = {
      new ForeachDatasetSink(func)
    }
  }

  /**
   * Custom sink similar to the old foreachRDD.
   * To use with the stream writer - do not construct directly, instead subclass
   * [[ForeachDatasetSinkProvider]] and provide to Spark's DataStreamWriter format.
   * Note: As with all sinks, `func` must not directly perform an DataFrame operations on
   * its input, instead it may convert the input to an RDD first or directly write the results out.
   */
  case class ForeachDatasetSink(func: DataFrame => Unit)
      extends Sink {
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
      func(data)
    }
  }


  test("Simple custom foreach") {
    withTempDir { checkpointDir =>
      val start = 0
      val end = 25

      val inputData = MemoryStream[Int]
      inputData.addData(start.to(end))

      val df = inputData.toDS()
        .flatMap(x => Iterator(x, x, x)).toDF("id")
        .select($"id", lit(100).as("data1"))

      var count = 0L
      var sum = 0L

      val foreachSink = new ForeachDatasetSinkProvider({df =>
        val rdd = df.rdd
        count += rdd.count()
        sum += rdd.collect().map(_.getInt(0)).sum
      })

      val writer = df.writeStream.format(foreachSink)
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
      val query = writer.start()
      assert(query.isActive === true)
      query.processAllAvailable()
      assert(0.to(end).sum * 3 === sum)
      assert(count === (end + 1) * 3)

      inputData.addData(start.to(end))
      query.processAllAvailable()
      assert(0.to(end).sum * 3 * 2 === sum)
    }
  }
}
