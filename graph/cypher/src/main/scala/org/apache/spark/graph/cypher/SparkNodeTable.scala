package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.okapi.relational.api.io.NodeTable

case class SparkNodeTable(override val mapping: NodeMapping, override val table: DataFrameTable)
  extends NodeTable(mapping, table) with RecordBehaviour {

  override type Records = SparkNodeTable

  override def cache(): SparkNodeTable = {
    table.cache()
    this
  }
}
