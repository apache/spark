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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.streaming.sources.ConsoleWrite
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ConsoleRelation(override val sqlContext: SQLContext, data: DataFrame)
  extends BaseRelation {
  override def schema: StructType = data.schema
}

class ConsoleSinkProvider extends SimpleTableProvider
  with DataSourceRegister
  with CreatableRelationProvider {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    ConsoleTable
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

object ConsoleTable extends Table with SupportsWrite {

  override def name(): String = "console"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.STREAMING_WRITE).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {
      private val inputSchema: StructType = info.schema()

      // Do nothing for truncate. Console sink is special and it just prints all the records.
      override def truncate(): WriteBuilder = this

      override def buildForStreaming(): StreamingWrite = {
        assert(inputSchema != null)
        new ConsoleWrite(inputSchema, info.options)
      }
    }
  }
}
