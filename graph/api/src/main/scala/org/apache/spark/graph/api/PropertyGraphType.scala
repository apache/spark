package org.apache.spark.graph.api

/**
  * Describes the structure of a [[PropertyGraph]].
  */
trait PropertyGraphType {
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

}
