package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkCypherEntity._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

object SparkCypherEntity {

  implicit class RichId(id: Seq[Byte]) {

    def toHex: String = s"0x${id.map(id => "%02X".format(id)).mkString}"

  }
}

case class SparkCypherNode(
  override val id: Seq[Byte],
  override val labels: Set[String] = Set.empty,
  override val properties: CypherMap = CypherMap.empty
) extends CypherNode[Seq[Byte]] {

  override type I = SparkCypherNode

  override def copy(
    id: Seq[Byte] = id,
    labels: Set[String] = labels,
    properties: CypherMap = properties
  ): SparkCypherNode = {
    SparkCypherNode(id, labels, properties)
  }

  override def toString: String = s"${getClass.getSimpleName}(id=${id.toHex}, labels=$labels, properties=$properties)"
}

case class SparkCypherRelationship(
  override val id: Seq[Byte],
  override val startId: Seq[Byte],
  override val endId: Seq[Byte],
  override val relType: String,
  override val properties: CypherMap = CypherMap.empty
) extends CypherRelationship[Seq[Byte]] {

  override type I = SparkCypherRelationship

  override def copy(
    id: Seq[Byte] = id,
    startId: Seq[Byte] = startId,
    endId: Seq[Byte] = endId,
    relType: String = relType,
    properties: CypherMap = properties
  ): SparkCypherRelationship = SparkCypherRelationship(id, startId, endId, relType, properties)

  override def toString: String = s"${getClass.getSimpleName}(id=${id.toHex}, startId=${startId.toHex}, endId=${endId.toHex}, relType=$relType, properties=$properties)"

}