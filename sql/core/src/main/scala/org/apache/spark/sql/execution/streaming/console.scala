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

import java.util.Optional

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.sources.{ConsoleContinuousWriter, ConsoleMicroBatchWriter, ConsoleWriter}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.streaming.{ContinuousWriteSupport, MicroBatchWriteSupport}
import org.apache.spark.sql.sources.v2.streaming.writer.ContinuousWriter
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

case class ConsoleRelation(override val sqlContext: SQLContext, data: DataFrame)
  extends BaseRelation {
  override def schema: StructType = data.schema
}

class ConsoleSinkProvider extends DataSourceV2
  with MicroBatchWriteSupport
  with ContinuousWriteSupport
  with DataSourceRegister
  with CreatableRelationProvider {

  override def createMicroBatchWriter(
      queryId: String,
      batchId: Long,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[DataSourceV2Writer] = {
    Optional.of(new ConsoleMicroBatchWriter(batchId, schema, options))
  }

  override def createContinuousWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[ContinuousWriter] = {
    Optional.of(new ConsoleContinuousWriter(schema, options))
  }

  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    // Number of rows to display, by default 20 rows
    val numRowsToShow = parameters.get("numRows").map(_.toInt).getOrElse(20)

    // Truncate the displayed data if it is too long, by default it is true
    val isTruncated = parameters.get("truncate").map(_.toBoolean).getOrElse(true)
    data.show(numRowsToShow, isTruncated)

    ConsoleRelation(sqlContext, data)
  }

  def shortName(): String = "console"
}
