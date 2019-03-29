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
}
