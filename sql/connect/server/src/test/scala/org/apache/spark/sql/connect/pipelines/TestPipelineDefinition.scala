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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.{proto => sc}

/**
 * A test class to simplify the registration of datasets for unit testing.
 */
class TestPipelineDefinition(graphId: String) {

  private[connect] val tableDefs =
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineDataset]()
  private[connect] val viewDefs =
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineDataset]()
  private[connect] val flowDefs =
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineFlow]()

  protected def createTable(
      name: String,
      datasetType: sc.DatasetType,
      query: Option[sc.Relation] = None,
      sparkConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      // TODO: Add support for specifiedSchema
      // specifiedSchema: Option[StructType] = None,
      partitionCols: Option[Seq[String]] = None,
      properties: Map[String, String] = Map.empty): Unit = {
    tableDefs += sc.PipelineCommand.DefineDataset
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setDatasetName(name)
      .setDatasetType(datasetType)
      .setComment(comment.getOrElse(""))
      .addAllPartitionCols(partitionCols.getOrElse(Seq()).asJava)
      .putAllTableProperties(properties.asJava)
      .build()

    query.foreach { q =>
      flowDefs += sc.PipelineCommand.DefineFlow
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFlowName(name)
        .setTargetDatasetName(name)
        .setRelation(q)
        .putAllSqlConf(sparkConf.asJava)
        .setOnce(false)
        .build()
    }
  }

  protected def createTable(
      name: String,
      datasetType: sc.DatasetType,
      sql: Option[String]): Unit = {
    createTable(
      name,
      datasetType,
      query = sql.map(s =>
        sc.Relation
          .newBuilder()
          .setSql(sc.SQL.newBuilder().setQuery(s).build())
          .build()))
  }

  protected def createView(
      name: String,
      query: sc.Relation,
      sparkConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      sqlText: Option[String] = None): Unit = {
    tableDefs += sc.PipelineCommand.DefineDataset
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setDatasetName(name)
      .setDatasetType(sc.DatasetType.TEMPORARY_VIEW)
      .setComment(comment.getOrElse(""))
      .build()

    flowDefs += sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(name)
      .setTargetDatasetName(name)
      .setRelation(query)
      .putAllSqlConf(sparkConf.asJava)
      .setOnce(false)
      .build()

  }

  protected def createView(name: String, sql: String): Unit = {
    createView(
      name,
      query = sc.Relation
        .newBuilder()
        .setSql(sc.SQL.newBuilder().setQuery(sql).build())
        .build())
  }

  protected def createFlow(
      name: String,
      destinationName: String,
      query: sc.Relation,
      sparkConf: Map[String, String] = Map.empty,
      once: Boolean = false): Unit = {
    flowDefs += sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(name)
      .setTargetDatasetName(destinationName)
      .setRelation(query)
      .putAllSqlConf(sparkConf.asJava)
      .setOnce(once)
      .build()
  }
}
