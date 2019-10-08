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

package org.apache.spark.cypher.io

import org.apache.spark.cypher.SparkCypherSession._
import org.apache.spark.cypher.SparkGraphDirectoryStructure
import org.apache.spark.cypher.io.ReadWriteGraph._
import org.apache.spark.graph.api.{PropertyGraph, PropertyGraphWriter}

class SparkCypherPropertyGraphWriter(graph: PropertyGraph) extends PropertyGraphWriter(graph) {

  override def save(path: String): Unit = {
    val relationalGraph = toRelationalGraph(graph)
    val graphDirectoryStructure = SparkGraphDirectoryStructure(path)

    relationalGraph.schema.labelCombinations.combos.foreach { combo =>
      relationalGraph.canonicalNodeTable(combo)
        .write
        .format(format)
        .mode(saveMode)
        .save(graphDirectoryStructure.pathToNodeTable(combo))
    }
    relationalGraph.schema.relationshipTypes.foreach { relType =>
      relationalGraph.canonicalRelationshipTable(relType)
        .write
        .format(format)
        .mode(saveMode)
        .save(graphDirectoryStructure.pathToRelationshipTable(relType))
    }
  }

}
