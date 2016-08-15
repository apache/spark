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
import org.apache.spark.sql.execution.streaming.StreamSinkProvider
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
  abstract class ForeachDatasetSinkProvider extends StreamSinkProvider {
    def func(df: DataFrame): Unit

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
   *  This can also be used directly as in StreamingNaiveBayes.scala
   */
  case class ForeachDatasetSink(func: DataFrame => Unit)
      extends Sink {

    val estimator = new StreamingNaiveBayes()

    override def addBatch(batchId: Long, data: DataFrame): Unit = {
      func(data)
    }
  }


  test("Simple custom foreach") {
    implicit val e = ExpressionEncoder[java.lang.Long]

    val start = 0
    val end = 200
    val numPartitions = 100

    val df = spark
      .range(start, end, 1, numPartitions)
      .flatMap(x => Iterator(x, x, x)).toDF("id")
      .select($"id", lit(100).as("data1"), lit(1000).as("data2"))

    var count = 0

    val foreachSink = new ForeachDatasetSink(df => count += df.rdd.count())

    val query = df.writeStream().format(foreachSink)
  }
}
