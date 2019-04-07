/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.cypher

import org.apache.spark.cypher.SparkCypherEntity._
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