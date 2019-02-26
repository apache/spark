package org.apache.spark.graph.cypher.adapters

import org.apache.spark.graph.api.{NodeDataFrame, RelationshipDataFrame}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}

object MappingAdapter {

  implicit class RichNodeDataFrame(val nodeDf: NodeDataFrame) extends AnyVal {
    def toNodeMapping: EntityMapping = NodeMappingBuilder
      .on(nodeDf.idColumn)
      .withImpliedLabels(nodeDf.labels.toSeq: _*)
      .withPropertyKeyMappings(nodeDf.properties.toSeq:_*)
      .build
  }

  implicit class RichRelationshipDataFrame(val relDf: RelationshipDataFrame) extends AnyVal {
    def toRelationshipMapping: EntityMapping = RelationshipMappingBuilder
        .on(relDf.idColumn)
        .withSourceStartNodeKey(relDf.sourceIdColumn)
        .withSourceEndNodeKey(relDf.targetIdColumn)
        .withRelType(relDf.relationshipType)
        .withPropertyKeyMappings(relDf.properties.toSeq: _*)
        .build
  }
}
