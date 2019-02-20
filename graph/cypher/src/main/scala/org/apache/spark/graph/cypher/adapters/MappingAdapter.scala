package org.apache.spark.graph.cypher.adapters

import org.apache.spark.graph.api.{NodeDataFrame, RelationshipDataFrame}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}

object MappingAdapter {

  implicit class RichNodeDataFrame(val nodeDf: NodeDataFrame) extends AnyVal {
    def toNodeMapping: NodeMapping =
      NodeMapping(nodeDf.idColumn, nodeDf.labels, nodeDf.optionalLabels, nodeDf.properties)

  }

  implicit class RichRelationshipDataFrame(val relDf: RelationshipDataFrame) extends AnyVal {
    def toRelationshipMapping: RelationshipMapping =
      RelationshipMapping(relDf.idColumn, relDf.sourceIdColumn, relDf.targetIdColumn, Left(relDf.relationshipType), relDf.properties)
  }
}
