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

import java.text.SimpleDateFormat
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.execution.streaming.continuous.TextSocketContinuousStream
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TextSocketSourceProvider extends TableProvider with DataSourceRegister with Logging {

  private def checkParameters(params: CaseInsensitiveStringMap): Unit = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!params.containsKey("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!params.containsKey("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    try {
      if (options.getBoolean("includeTimestamp", false)) {
        TextSocketReader.SCHEMA_TIMESTAMP
      } else {
        TextSocketReader.SCHEMA_REGULAR
      }
    } catch {
      case NonFatal(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  override def inferPartitioning(
      schema: StructType, options: CaseInsensitiveStringMap): Array[Transform] = {
    Array.empty
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val actualSchema = inferSchema(options)
    if (schema != actualSchema) {
      throw new IllegalArgumentException(
        s"""
          |Specified schema does not match the actual table schema of text socket source:
          |Specified schema:    $schema
          |Actual table schema: $actualSchema
        """.stripMargin)
    }

    if (partitioning.nonEmpty) {
      throw new IllegalArgumentException("text socket source does not support partitioning.")
    }

    checkParameters(options)
    new TextSocketTable(
      schema,
      options.get("host"),
      options.getInt("port", -1),
      options.getInt("numPartitions", SparkSession.active.sparkContext.defaultParallelism))
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}

class TextSocketTable(
    override val schema: StructType,
    host: String,
    port: Int,
    numPartitions: Int)
  extends Table with SupportsRead {

  override def name(): String = s"Socket[$host:$port]"

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new TextSocketMicroBatchStream(host, port, numPartitions)
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      new TextSocketContinuousStream(host, port, numPartitions, options)
    }
  }
}

object TextSocketReader {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}
