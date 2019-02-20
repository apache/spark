package org.apache.spark.graph.api

trait CypherEngine {
  def cypher(graph: PropertyGraph, query: String): CypherResult

  def createGraph(
    nodes: Seq[NodeDataFrame],
    relationships: Seq[RelationshipDataFrame] = Seq.empty
  ): PropertyGraph
}
