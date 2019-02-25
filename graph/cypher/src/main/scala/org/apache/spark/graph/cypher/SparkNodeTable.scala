package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.okapi.relational.api.io.NodeTable

case class SparkNodeTable(mapping: NodeMapping, table: DataFrameTable)
  extends NodeTable(mapping, table) with RecordBehaviour {

  override type Records = SparkNodeTable

  override def cache(): SparkNodeTable = {
    table.cache()
    this
  }

}


object SparkNodeTable {

  def create(mapping: NodeMapping, table: DataFrameTable): SparkNodeTable = {
    val columnNames = table.df.columns
    val columnRenames = columnNames.zip(columnNames.map(escape)).toMap
    val escapedTable = table.df.withColumnsRenamed(columnRenames)
    val escapedMapping = mapping match {
      case NodeMapping(id, labels, optionalLabels, properties) =>
        NodeMapping(escape(id), labels, optionalLabels.mapValues(escape), properties.mapValues(escape))
    }
    SparkNodeTable(escapedMapping, escapedTable)
  }

  // TODO: Ensure that there are no conflicts with existing column names
  private def escape(columnName: String): String = {
    columnName.replaceAll("\\.", "_DOT_")
  }

}
