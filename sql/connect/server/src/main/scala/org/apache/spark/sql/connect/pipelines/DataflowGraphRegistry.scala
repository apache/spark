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
 */
package org.apache.spark.sql.connect.pipelines

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.pipelines.graph.GraphRegistrationContext

object DataflowGraphRegistry {

  private val dataflowGraphs = new ConcurrentHashMap[String, GraphRegistrationContext]()

  def createDataflowGraph(
      defaultCatalog: String,
      defaultDatabase: String,
      defaultSqlConf: Map[String, String]): String = {
    val graphId = java.util.UUID.randomUUID().toString
    // TODO: propagate pipeline catalog and schema from pipeline spec here.
    dataflowGraphs.put(
      graphId,
      new GraphRegistrationContext(defaultCatalog, defaultDatabase, defaultSqlConf)
    )
    graphId
  }

  def getDataflowGraph(pipelineId: String): Option[GraphRegistrationContext] = {
    Option(dataflowGraphs.get(pipelineId))
  }

  def getDataflowGraphOrThrow(dataflowGraphId: String): GraphRegistrationContext =
    DataflowGraphRegistry.getDataflowGraph(dataflowGraphId).getOrElse {
      throw new IllegalArgumentException(
        s"Pipeline context with ID $dataflowGraphId does not exist."
      )
    }

  def dropDataflowGraph(pipelineId: String): Unit = {
    dataflowGraphs.remove(pipelineId)
  }

  def getAllDataflowGraphs: Seq[GraphRegistrationContext] = {
    dataflowGraphs.values().asScala.toSeq
  }

  def dropAllDataflowGraphs(): Unit = {
    dataflowGraphs.clear()
  }
}
