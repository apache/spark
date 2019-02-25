package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.io.RelationshipTable

case class SparkRelationshipTable(mapping: RelationshipMapping, table: DataFrameTable)
  extends RelationshipTable(mapping, table) with RecordBehaviour {

  override type Records = SparkRelationshipTable

  override def cache(): SparkRelationshipTable = {
    table.cache()
    this
  }
}

object SparkRelationshipTable {

  def create(mapping: RelationshipMapping, table: DataFrameTable): SparkRelationshipTable = {
    val columnNames = table.df.columns
    val columnRenames = columnNames.zip(columnNames.map(escape)).toMap
    val escapedTable = table.df.withColumnsRenamed(columnRenames)
    val escapedMapping = mapping match {
      case RelationshipMapping(id, start, end, relType@Left(_), properties) =>
        RelationshipMapping(escape(id), escape(start), escape(end), relType, properties.mapValues(escape))
      case _ =>
        throw UnsupportedOperationException("Relationship types in a string column")
    }
    SparkRelationshipTable(escapedMapping, escapedTable)
  }

  // TODO: Ensure that there are no conflicts with existing column names
  private def escape(columnName: String): String = {
    columnName.replaceAll("\\.", "_DOT_")
  }

}
