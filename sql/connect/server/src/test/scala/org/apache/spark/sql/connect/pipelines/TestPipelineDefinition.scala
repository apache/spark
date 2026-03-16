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
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineOutput]()
  private[connect] val viewDefs =
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineOutput]()
  private[connect] val flowDefs =
    new scala.collection.mutable.ArrayBuffer[sc.PipelineCommand.DefineFlow]()

  protected def createTable(
      name: String,
      outputType: sc.OutputType,
      query: Option[sc.Relation] = None,
      sparkConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      // TODO: Add support for specifiedSchema
      // specifiedSchema: Option[StructType] = None,
      partitionCols: Option[Seq[String]] = None,
      clusterCols: Option[Seq[String]] = None,
      properties: Map[String, String] = Map.empty): Unit = {
    val tableDetails = sc.PipelineCommand.DefineOutput.TableDetails
      .newBuilder()
      .addAllPartitionCols(partitionCols.getOrElse(Seq()).asJava)
      .addAllClusteringColumns(clusterCols.getOrElse(Seq()).asJava)
      .putAllTableProperties(properties.asJava)
      .build()

    tableDefs += sc.PipelineCommand.DefineOutput
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setOutputName(name)
      .setOutputType(outputType)
      .setComment(comment.getOrElse(""))
      .setTableDetails(tableDetails)
      .build()

    query.foreach { q =>
      val relationFlowDetails = sc.PipelineCommand.DefineFlow.WriteRelationFlowDetails
        .newBuilder()
        .setRelation(q)
        .build()

      flowDefs += sc.PipelineCommand.DefineFlow
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFlowName(name)
        .setTargetDatasetName(name)
        .setRelationFlowDetails(relationFlowDetails)
        .putAllSqlConf(sparkConf.asJava)
        .build()
    }
  }

  protected def createTable(
      name: String,
      outputType: sc.OutputType,
      sql: Option[String]): Unit = {
    createTable(
      name,
      outputType,
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
    tableDefs += sc.PipelineCommand.DefineOutput
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setOutputName(name)
      .setOutputType(sc.OutputType.TEMPORARY_VIEW)
      .setComment(comment.getOrElse(""))
      .build()

    val relationFlowDetails = sc.PipelineCommand.DefineFlow.WriteRelationFlowDetails
      .newBuilder()
      .setRelation(query)
      .build()

    flowDefs += sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(name)
      .setTargetDatasetName(name)
      .setRelationFlowDetails(relationFlowDetails)
      .putAllSqlConf(sparkConf.asJava)
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
    val relationFlowDetails = sc.PipelineCommand.DefineFlow.WriteRelationFlowDetails
      .newBuilder()
      .setRelation(query)
      .build()

    flowDefs += sc.PipelineCommand.DefineFlow
      .newBuilder()
      .setDataflowGraphId(graphId)
      .setFlowName(name)
      .setTargetDatasetName(destinationName)
      .setRelationFlowDetails(relationFlowDetails)
      .putAllSqlConf(sparkConf.asJava)
      .build()
  }
}
