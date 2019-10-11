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
 *
 */

package org.apache.spark.cypher

import java.util.Collections

import org.apache.spark.cypher.SparkTable.DataFrameTable
import org.apache.spark.cypher.conversions.CypherValueEncoders._
import org.apache.spark.cypher.conversions.RowConversion
import org.apache.spark.cypher.conversions.TypeConversions._
import org.apache.spark.sql._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory}
import org.opencypher.okapi.relational.impl.table._

import scala.collection.JavaConverters._

case class SparkCypherRecordsFactory()(implicit caps: SparkCypherSession) extends RelationalCypherRecordsFactory[DataFrameTable] {

  override type Records = SparkCypherRecords

  override def unit(): SparkCypherRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    SparkCypherRecords(RecordHeader.empty, initialDataFrame)
  }

  override def empty(initialHeader: RecordHeader = RecordHeader.empty): SparkCypherRecords = {
    val initialSparkStructType = initialHeader.toStructType
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    SparkCypherRecords(initialHeader, initialDataFrame)
  }

  override def fromElementTable(entityTable: ElementTable[DataFrameTable]): SparkCypherRecords = {
    SparkCypherRecords(entityTable.header, entityTable.table.df)
  }

  override def from(
    header: RecordHeader,
    table: DataFrameTable,
    maybeDisplayNames: Option[Seq[String]]
  ): SparkCypherRecords = {
    val displayNames = maybeDisplayNames match {
      case s@Some(_) => s
      case None => Some(header.vars.map(_.withoutType).toSeq)
    }
    SparkCypherRecords(header, table, displayNames)
  }

  private case class EmptyRow()
}

case class SparkCypherRecords(
  header: RecordHeader,
  table: DataFrameTable,
  override val logicalColumns: Option[Seq[String]] = None
)(implicit session: SparkCypherSession) extends RelationalCypherRecords[DataFrameTable] with RecordBehaviour {
  override type Records = SparkCypherRecords

  def ds: Dataset[Row] = table.df

  override def cache(): SparkCypherRecords = {
    ds.cache()
    this
  }

  override def toString: String = {
    if (header.isEmpty) {
      s"CAPSRecords.empty"
    } else {
      s"CAPSRecords(header: $header)"
    }
  }
}

trait RecordBehaviour extends RelationalCypherRecords[DataFrameTable] {

  override lazy val columnType: Map[String, CypherType] = table.df.columnType

  override def rows: Iterator[String => CypherValue] = {
    toLocalIterator.asScala.map(_.value)
  }

  override def iterator: Iterator[CypherMap] = {
    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CypherMap] = {
    toCypherMaps.toLocalIterator()
  }

  def foreachPartition(f: Iterator[CypherMap] => Unit): Unit = {
    toCypherMaps.foreachPartition(f)
  }

  override def collect: Array[CypherMap] = {
    toCypherMaps.collect()
  }

  def toCypherMaps: Dataset[CypherMap] = {
    table.df.map(RowConversion(header.exprToColumn.toSeq))
  }
}
