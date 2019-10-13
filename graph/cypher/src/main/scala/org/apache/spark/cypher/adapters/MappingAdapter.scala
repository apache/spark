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

package org.apache.spark.cypher.adapters

import org.apache.spark.graph.api.{NodeDataset, RelationshipDataset}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}

object MappingAdapter {

  implicit class RichNodeDataDataset(val nodeDf: NodeDataset) extends AnyVal {
    def toNodeMapping: ElementMapping = NodeMappingBuilder
      .on(nodeDf.idColumn)
      .withImpliedLabels(nodeDf.labelSet.toSeq: _*)
      .withPropertyKeyMappings(nodeDf.properties.toSeq:_*)
      .build
  }

  implicit class RichRelationshipDataDataset(val relDf: RelationshipDataset) extends AnyVal {
    def toRelationshipMapping: ElementMapping = RelationshipMappingBuilder
        .on(relDf.idColumn)
        .withSourceStartNodeKey(relDf.sourceIdColumn)
        .withSourceEndNodeKey(relDf.targetIdColumn)
        .withRelType(relDf.relationshipType)
        .withPropertyKeyMappings(relDf.properties.toSeq: _*)
        .build
  }
}
