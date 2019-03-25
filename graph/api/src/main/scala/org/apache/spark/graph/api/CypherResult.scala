package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

/**
  * Result of a Cypher query.
  *
  * Wraps a [[DataFrame]] that contains the result rows.
  */
trait CypherResult {
  /**
    * Contains the result rows.
    *
    * The column names are aligned with the return item names specified within the Cypher query,
    * (e.g. `RETURN foo, bar AS baz` results in the columns `foo` and `baz`).
    *
    * @note Dot characters (i.e. `.`) within return item names are replaced by the underscore (i.e. `_`),
    *       (e.g. `MATCH (n:Person) RETURN n` results in the columns `n`, `n:Person` and `n_name`).
    */
  def df: DataFrame

  // TODO: determine what happens if there are dangling relationships (create empty nodes or filter such relationships)
  /**
    * Creates a [[PropertyGraph]] from nodes and relationship present in this result.
    *
    * @example
    * {{{
    * val result = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    * // returns the original graph
    * val graph2 = result.graph
    * }}}
    */
  def graph: PropertyGraph

  /**
    * Extracts nodes that are specified as a return item.
    *
    * @example
    * {{{
    * val result = graph.cypher("MATCH (n:Person)-[r:LIVES_IN]->(c:City) RETURN n, r, c")
    * val nodeFrames = result.nodeFrames("n")
    * }}}
    *
    * Returns a sequence of [[NodeFrame]]s where each single [[NodeFrame]] contains all distinct node entities that have at least label `:Person`.
    * For example, if the graph contains nodes with label `:Person` and `:Person:Fireman` the sequence contains two [[NodeFrame]]s, one
    * for each label combination.
    *
    * @param varName return item name of the node to extract
    */
  def nodeFrames(varName: String): Seq[NodeFrame]

  /**
    * Extracts relationships that are specified as a return item.
    *
    * @example
    * {{{
    * val result = graph.cypher("MATCH (n:Person)-[r:LIVES_IN|WORKS_IN]->(c:City) RETURN n, r, c")
    * val nodeFrames = result.nodeFrames("r")
    * }}}
    *
    * Returns a sequence of two [[RelationshipFrame]]s, one that contains all distinct relationships with type `:LIVES_IN` and another for type `:WORKS_IN`.
    *
    * @param varName return item name of the relationships to extract
    */
  def relationshipFrames(varName: String): Seq[RelationshipFrame]
}
