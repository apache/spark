package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.EntityMapping
import org.opencypher.okapi.relational.api.io.EntityTable

case class SparkEntityTable(
  override val mapping: EntityMapping,
  override val table: DataFrameTable
) extends EntityTable[DataFrameTable] with RecordBehaviour {

  override type Records = SparkEntityTable

  private[spark] def records(implicit cypherEngine: SparkCypherSession): SparkCypherRecords = cypherEngine.records.fromEntityTable(entityTable = this)

  override def cache(): SparkEntityTable = {
    table.cache()
    this
  }
}
