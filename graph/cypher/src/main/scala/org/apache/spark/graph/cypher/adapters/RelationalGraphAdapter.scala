package org.apache.spark.graph.cypher.adapters

import org.apache.spark.graph.api.{CypherEngine, PropertyGraph}
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph

case class RelationalGraphAdapter(
  cypherEngine: CypherEngine,
  graph: RelationalCypherGraph[DataFrameTable]) extends PropertyGraph {

  override def nodes: DataFrame = graph.nodes("n").table.df

  override def relationships: DataFrame = graph.relationships("r").table.df
}
