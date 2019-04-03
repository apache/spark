package org.apache.spark.cypher.adapters

import org.apache.spark.graph.api.{NodeFrame, RelationshipFrame}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}

object MappingAdapter {

  implicit class RichNodeDataFrame(val nodeDf: NodeFrame) extends AnyVal {
    def toNodeMapping: EntityMapping = NodeMappingBuilder
      .on(nodeDf.idColumn)
      .withImpliedLabels(nodeDf.labels.toSeq: _*)
      .withPropertyKeyMappings(nodeDf.properties.toSeq:_*)
      .build
  }

  implicit class RichRelationshipDataFrame(val relDf: RelationshipFrame) extends AnyVal {
    def toRelationshipMapping: EntityMapping = RelationshipMappingBuilder
        .on(relDf.idColumn)
        .withSourceStartNodeKey(relDf.sourceIdColumn)
        .withSourceEndNodeKey(relDf.targetIdColumn)
        .withRelType(relDf.relationshipType)
        .withPropertyKeyMappings(relDf.properties.toSeq: _*)
        .build
  }
}
