package org.apache.spark.graph.api

import org.apache.spark.sql.SparkSession

/**
  * Allows for creating [[PropertyGraph]] instances and running Cypher-queries on them.
  */
trait CypherSession {

  def sparkSession: SparkSession

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph      [[PropertyGraph]] on which the query is executed
    * @param query      Cypher query to execute
    */
  def cypher(graph: PropertyGraph, query: String): CypherResult

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph      [[PropertyGraph]] on which the query is executed
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    */
  def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult

  /**
    * Creates a [[PropertyGraph]] from a sequence of [[NodeFrame]]s and [[RelationshipFrame]]s.
    * At least one [[NodeFrame]] has to be provided.
    *
    * @param nodes         [[NodeFrame]]s that define the nodes in the graph
    * @param relationships [[RelationshipFrame]]s that define the relationships in the graph
    */
  def createGraph(nodes: Seq[NodeFrame], relationships: Seq[RelationshipFrame]): PropertyGraph
}
