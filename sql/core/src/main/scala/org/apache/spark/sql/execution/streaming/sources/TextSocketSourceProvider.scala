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
import java.util.Locale

import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.continuous.TextSocketContinuousStream
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{Scan, ScanBuilder}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

class TextSocketSourceProvider extends TableProvider with DataSourceRegister with Logging {

  private def checkParameters(params: DataSourceOptions): Unit = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!params.get("host").isPresent) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!params.get("port").isPresent) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    Try {
      params.get("includeTimestamp").orElse("false").toBoolean
    } match {
      case Success(_) =>
      case Failure(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  override def getTable(options: DataSourceOptions): Table = {
    checkParameters(options)
    new TextSocketTable(
      options.get("host").get,
      options.getInt("port", -1),
      options.getInt("numPartitions", SparkSession.active.sparkContext.defaultParallelism),
      options.getBoolean("includeTimestamp", false))
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}

class TextSocketTable(host: String, port: Int, numPartitions: Int, includeTimestamp: Boolean)
  extends Table with SupportsMicroBatchRead with SupportsContinuousRead {

  override def name(): String = s"Socket[$host:$port]"

  override def schema(): StructType = {
    if (includeTimestamp) {
      TextSocketReader.SCHEMA_TIMESTAMP
    } else {
      TextSocketReader.SCHEMA_REGULAR
    }
  }

  override def newScanBuilder(options: DataSourceOptions): ScanBuilder = new ScanBuilder {
    override def build(): Scan = new Scan {
      override def readSchema(): StructType = schema()

      override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
        new TextSocketMicroBatchStream(host, port, numPartitions, options)
      }

      override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
        new TextSocketContinuousStream(host, port, numPartitions, options)
      }
    }
  }
}

object TextSocketReader {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}
