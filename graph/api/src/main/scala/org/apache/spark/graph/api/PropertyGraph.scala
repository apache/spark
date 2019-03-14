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
    * Returns all labels occurring on any node in the graph.
    */
  def labels: Set[String] = labelSets.flatten

  /**
    * Returns all distinct label sets occurring on nodes in the graph.
    */
  def labelSets: Set[Set[String]]

  /**
    * Returns all relationship types occurring on relationships in the graph.
    */
  def relationshipTypes: Set[String]

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
    * Returns the [[NodeFrame]] for a given node label set.
    *
    * @param labelSet Label set used for [[NodeFrame]] lookup
    * @return [[NodeFrame]] for the given label set
    */
  def nodeFrame(labelSet: Set[String]): NodeFrame

  /**
    * Returns the [[RelationshipFrame]] for a given relationship type.
    *
    * @param relationshipType Relationship type used for [[RelationshipFrame]] lookup
    * @return [[RelationshipFrame]] for the given relationship type
    */
  def relationshipFrame(relationshipType: String): RelationshipFrame

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
