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

import org.apache.spark.SparkException
import org.apache.spark.sql.pipelines.graph.GraphRegistrationContext

/**
 * Tracks the DataflowGraphs that have been registered. DataflowGraphs are registered by the
 * PipelinesHandler when CreateDataflowGraph is called, and the PipelinesHandler also supports
 * attaching flows/datasets to a graph.
 */
// TODO(SPARK-51727): Currently DataflowGraphRegistry is a singleton, but it should instead be
//  scoped to a single SparkSession for proper isolation between pipelines that are run on the
//  same cluster.
object DataflowGraphRegistry {

  private val dataflowGraphs = new ConcurrentHashMap[String, GraphRegistrationContext]()

  /** Registers a DataflowGraph and generates a unique id to associate with the graph */
  def createDataflowGraph(
      defaultCatalog: String,
      defaultDatabase: String,
      defaultSqlConf: Map[String, String]): String = {
    val graphId = java.util.UUID.randomUUID().toString
    // TODO: propagate pipeline catalog and schema from pipeline spec here.
    dataflowGraphs.put(
      graphId,
      new GraphRegistrationContext(defaultCatalog, defaultDatabase, defaultSqlConf))
    graphId
  }

  /** Retrieves the graph for a given id. */
  def getDataflowGraph(graphId: String): Option[GraphRegistrationContext] = {
    Option(dataflowGraphs.get(graphId))
  }

  /** Retrieves the graph for a given id, and throws if the id could not be found. */
  def getDataflowGraphOrThrow(dataflowGraphId: String): GraphRegistrationContext =
    DataflowGraphRegistry.getDataflowGraph(dataflowGraphId).getOrElse {
      throw new SparkException(
        errorClass = "DATAFLOW_GRAPH_NOT_FOUND",
        messageParameters = Map("graphId" -> dataflowGraphId),
        cause = null)
    }

  /** Removes the graph with a given id from the registry. */
  def dropDataflowGraph(graphId: String): Unit = {
    dataflowGraphs.remove(graphId)
  }

  /** Returns all graphs in the registry. */
  def getAllDataflowGraphs: Seq[GraphRegistrationContext] = {
    dataflowGraphs.values().asScala.toSeq
  }

  /** Removes all graphs from the registry. */
  def dropAllDataflowGraphs(): Unit = {
    dataflowGraphs.clear()
  }
}
