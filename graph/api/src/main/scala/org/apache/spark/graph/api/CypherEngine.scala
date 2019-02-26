package org.apache.spark.graph.api

trait CypherEngine {
  def cypher(graph: PropertyGraph, query: String): CypherResult

  def createGraph(
    nodes: Seq[NodeFrame],
    relationships: Seq[RelationshipFrame] = Seq.empty
  ): PropertyGraph
}
