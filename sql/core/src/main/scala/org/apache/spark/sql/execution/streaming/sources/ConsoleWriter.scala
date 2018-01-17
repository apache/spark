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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.DataSourceV2Options
import org.apache.spark.sql.sources.v2.writer.{DataSourceV2Writer, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ConsoleWriter(batchId: Long, schema: StructType, options: DataSourceV2Options)
    extends DataSourceV2Writer with Logging {
  // Number of rows to display, by default 20 rows
  private val numRowsToShow = options.getInt("numRows", 20)

  // Truncate the displayed data if it is too long, by default it is true
  private val isTruncated = options.getBoolean("truncate", true)

  assert(SparkSession.getActiveSession.isDefined)
  private val spark = SparkSession.getActiveSession.get

  override def createWriterFactory(): DataWriterFactory[Row] = PackedRowWriterFactory

  override def commit(messages: Array[WriterCommitMessage]): Unit = synchronized {
    val batch = messages.collect {
      case PackedRowCommitMessage(rows) => rows
    }.fold(Array())(_ ++ _)

    // scalastyle:off println
    println("-------------------------------------------")
    println(s"Batch: $batchId")
    println("-------------------------------------------")
    // scalastyle:off println
    spark.createDataFrame(
      spark.sparkContext.parallelize(batch), schema)
      .show(numRowsToShow, isTruncated)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def toString(): String = s"ConsoleWriter[numRows=$numRowsToShow, truncate=$isTruncated]"
}
