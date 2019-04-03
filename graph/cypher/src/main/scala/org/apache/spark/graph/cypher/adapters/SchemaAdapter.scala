package org.apache.spark.graph.cypher.adapters

import org.apache.spark.graph.api.PropertyGraphType
import org.opencypher.okapi.api.schema.Schema

case class SchemaAdapter(schema: Schema) extends PropertyGraphType {

  override def labelSets: Set[Set[String]] = schema.labelCombinations.combos

  override def relationshipTypes: Set[String] = schema.relationshipTypes


}
