package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.relational.api.io.RelationshipTable

case class SparkRelationshipTable(override val mapping: RelationshipMapping, override val table: DataFrameTable)
  extends RelationshipTable(mapping, table) with RecordBehaviour {

  override type Records = SparkRelationshipTable

  override def cache(): SparkRelationshipTable = {
    table.cache()
    this
  }
}
