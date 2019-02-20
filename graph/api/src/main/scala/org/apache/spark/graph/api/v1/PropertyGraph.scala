package org.apache.spark.graph.api.v1

import org.apache.spark.sql.DataFrame

trait PropertyGraph {
  def cypherEngine: CypherEngine

  def cypher(query: String): CypherResult =
    cypherEngine.cypher(this, query)

  def nodes: DataFrame
  def vertices: DataFrame = nodes
  def relationships: DataFrame
  def edges: DataFrame = relationships
}
