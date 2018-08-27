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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.continuous.TextSocketContinuousInputStream
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object TextSocketReader {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

class TextSocketSourceProvider extends Format with DataSourceRegister with Logging {

  override def getTable(options: DataSourceOptions): TextSocketTable = {
    new TextSocketTable(options)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}

class TextSocketTable(options: DataSourceOptions) extends Table
  with SupportsMicroBatchRead with SupportsContinuousRead with Logging {

  override def schema(): StructType = {
    if (options.getBoolean("includeTimestamp", false)) {
      TextSocketReader.SCHEMA_TIMESTAMP
    } else {
      TextSocketReader.SCHEMA_REGULAR
    }
  }

  override def createMicroBatchInputStream(
      checkpointLocation: String,
      config: ScanConfig,
      options: DataSourceOptions): MicroBatchInputStream = {
    checkParameters(options)
    new TextSocketMicroBatchInputStream(options)
  }

  override def createContinuousInputStream(
      checkpointLocation: String,
      config: ScanConfig,
      options: DataSourceOptions): ContinuousInputStream = {
    checkParameters(options)
    new TextSocketContinuousInputStream(options)
  }

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
}
