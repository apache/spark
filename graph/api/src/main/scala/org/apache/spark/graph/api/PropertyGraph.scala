package org.apache.spark.graph.api

import org.apache.spark.graph.api.io.PropertyGraphWriter
import org.apache.spark.sql.DataFrame

/**
  * A Property Graph as defined by the openCypher Property Graph Model.
  *
  * A graph is always tied to and managed by a [[CypherSession]]. The lifetime of a graph is bounded by the session lifetime.
  *
  * @see [[https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc openCypher Property Graph Model]]
  */
trait PropertyGraph {

  /**
    * The session in which this graph is managed.
    */
  def cypherSession: CypherSession

  /**
    * Executes a Cypher query in the session that manages this graph, using this graph as the input graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    */
  def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = cypherSession.cypher(this, query, parameters)

  /**
    * Returns a [[DataFrame]] that contains a row for each node in this graph.
    */
  def nodes: DataFrame

  /**
    * Returns a [[DataFrame]] that contains a row for each relationship in this graph.
    */
  def relationships: DataFrame

  /**
    * Returns a [[PropertyGraphWriter]].
    */
  def write: PropertyGraphWriter = cypherSession.write(this)

}
