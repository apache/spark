package org.apache.spark.graph.cypher

import java.util.Collections

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.conversions.RowConversion
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.graph.cypher.conversions.CypherValueEncoders._
import org.apache.spark.sql._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory}
import org.opencypher.okapi.relational.impl.table._

import scala.collection.JavaConverters._

case class SparkCypherRecordsFactory(implicit caps: SparkCypherSession) extends RelationalCypherRecordsFactory[DataFrameTable] {

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

  override def fromEntityTable(entityTable: EntityTable[DataFrameTable]): SparkCypherRecords = {
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

  //  /**
  //    * Wraps a Spark SQL table (DataFrame) in a CAPSRecords, making it understandable by Cypher.
  //    *
  //    * @param df   table to wrap.
  //    * @param caps session to which the resulting CAPSRecords is tied.
  //    * @return a Cypher table.
  //    */
  //  private[spark] def wrap(df: DataFrame)(implicit caps: SparkCypherSession): CAPSRecords = {
  //    val compatibleDf = df.withCypherCompatibleTypes
  //    CAPSRecords(compatibleDf.schema.toRecordHeader, compatibleDf)
  //  }

  private case class EmptyRow()
}

case class SparkCypherRecords(
  header: RecordHeader,
  table: DataFrameTable,
  override val logicalColumns: Option[Seq[String]] = None
)(implicit session: SparkCypherSession) extends RelationalCypherRecords[DataFrameTable] with RecordBehaviour {
  override type Records = SparkCypherRecords

  def df: DataFrame = table.df

  override def cache(): SparkCypherRecords = {
    df.cache()
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

