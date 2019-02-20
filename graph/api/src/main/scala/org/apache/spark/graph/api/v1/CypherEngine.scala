package org.apache.spark.graph.api.v1

trait CypherEngine {
  def cypher(graph: PropertyGraph, query: String): CypherResult

  def createGraph(
    nodes: Seq[NodeDataFrame],
    relationships: Seq[RelationshipDataFrame]
  ): PropertyGraph
}
