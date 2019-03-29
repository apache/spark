package org.apache.spark.graph.cypher

import org.apache.spark.graph.api.CypherResult
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords

case class SparkCypherResult(relationalTable: RelationalCypherRecords[DataFrameTable]) extends CypherResult {
  override val df: DataFrame = relationalTable.table.df
}
