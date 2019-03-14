package org.apache.spark.graph.cypher.adapters

import org.apache.spark.graph.api.{NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.adapters.MappingAdapter._
import org.apache.spark.graph.cypher.{SparkCypherSession, SparkEntityTable}
import org.apache.spark.sql.DataFrame

case class RelationalGraphAdapter(
  cypherSession: SparkCypherSession,
  nodeFrames: Seq[NodeFrame],
  relationshipFrames: Seq[RelationshipFrame]) extends PropertyGraph {

  private [graph] lazy val graph = {
    val nodeTables = nodeFrames.map { nodeDataFrame => SparkEntityTable(nodeDataFrame.toNodeMapping, nodeDataFrame.df) }
    val relTables = relationshipFrames.map { relDataFrame => SparkEntityTable(relDataFrame.toRelationshipMapping, relDataFrame.df) }
    cypherSession.graphs.create(nodeTables.head, nodeTables.tail ++ relTables: _*)
  }

  private lazy val _nodeFrame: Map[Set[String], NodeFrame] = nodeFrames.map(nf => nf.labels -> nf).toMap

  private lazy val _relationshipFrame: Map[String, RelationshipFrame] = relationshipFrames.map(rf => rf.relationshipType -> rf).toMap

  override def nodes: DataFrame = graph.nodes("n").table.df

  override def relationships: DataFrame = graph.relationships("r").table.df

  override def labelSets: Set[Set[String]] = graph.schema.labelCombinations.combos

  override def relationshipTypes: Set[String] = graph.schema.relationshipTypes

  override def nodeFrame(labelSet: Set[String]): NodeFrame = _nodeFrame(labelSet)

  override def relationshipFrame(relationshipType: String): RelationshipFrame = _relationshipFrame(relationshipType)

}
