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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class ConsoleSink(options: Map[String, String]) extends Sink with Logging {
  // Number of rows to display, by default 20 rows
  private val numRowsToShow = options.get("numRows").map(_.toInt).getOrElse(20)

  // Truncate the displayed data if it is too long, by default it is true
  private val isTruncated = options.get("truncate").map(_.toBoolean).getOrElse(true)

  // Track the batch id
  private var lastBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val batchIdStr = if (batchId <= lastBatchId) {
      s"Rerun batch: $batchId"
    } else {
      lastBatchId = batchId
      s"Batch: $batchId"
    }

    // scalastyle:off println
    println("-------------------------------------------")
    println(batchIdStr)
    println("-------------------------------------------")
    // scalastyle:off println
    data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
      .show(numRowsToShow, isTruncated)
  }
}

class ConsoleSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new ConsoleSink(parameters)
  }

  def shortName(): String = "console"
}
