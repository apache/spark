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
package org.apache.spark.sql.pipelines.graph

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * A mutable context for registering tables, views, and flows in a dataflow graph.
 *
 * @param defaultCatalog The pipeline's default catalog.
 * @param defaultDatabase The pipeline's default schema.
 */
class GraphRegistrationContext(
    val defaultCatalog: String,
    val defaultDatabase: String,
    val defaultSqlConf: Map[String, String]) {
  import GraphRegistrationContext._

  protected val tables = new mutable.ListBuffer[Table]
  protected val views = new mutable.ListBuffer[View]
  protected val sinks = new mutable.ListBuffer[Sink]
  protected val flows = new mutable.ListBuffer[UnresolvedFlow]

  def registerTable(tableDef: Table): Unit = {
    tables += tableDef
  }

  def registerView(viewDef: View): Unit = {
    views += viewDef
  }

  def registerSink(sinkDef: Sink): Unit = {
    sinks += sinkDef
  }

  def getViews: Seq[View] = {
    views.toSeq
  }

  def getSinks: Seq[Sink] = {
    sinks.toSeq
  }

  def registerFlow(flowDef: UnresolvedFlow): Unit = {
    flows += flowDef.copy(sqlConf = defaultSqlConf ++ flowDef.sqlConf)
  }

  private def isEmpty: Boolean = {
    tables.isEmpty && views.collect { case v: PersistedView =>
      v
    }.isEmpty && sinks.isEmpty
  }

  def toDataflowGraph: DataflowGraph = {
    if (isEmpty) {
      throw new AnalysisException(
        errorClass = "RUN_EMPTY_PIPELINE",
        messageParameters = Map.empty)
    }

    assertNoDuplicates(
      qualifiedTables = tables.toSeq,
      validatedViews = views.toSeq,
      qualifiedFlows = flows.toSeq,
      validatedSinks = sinks.toSeq
    )

    new DataflowGraph(
      tables = tables.toSeq,
      views = views.toSeq,
      sinks = sinks.toSeq,
      flows = flows.toSeq
    )
  }

  private def assertNoDuplicates(
      qualifiedTables: Seq[Table],
      validatedViews: Seq[View],
      validatedSinks: Seq[Sink],
      qualifiedFlows: Seq[UnresolvedFlow]): Unit = {

    (qualifiedTables.map(_.identifier) ++ validatedViews.map(_.identifier))
      .foreach { identifier =>
        assertOutputIdentifierIsUnique(
          identifier = identifier,
          tables = qualifiedTables,
          sinks = validatedSinks,
          views = validatedViews
        )
      }

    qualifiedFlows.foreach { flow =>
      assertFlowIdentifierIsUnique(
        flow = flow,
        flows = qualifiedFlows
      )
    }
  }

  private def assertOutputIdentifierIsUnique(
      identifier: TableIdentifier,
      tables: Seq[Table],
      sinks: Seq[Sink],
      views: Seq[View]): Unit = {

    // We need to check for duplicates in both tables and views, as they can have the same name.
    val allOutputs = tables.map(t => t.identifier -> TableType) ++ views.map(
        v => v.identifier -> ViewType
      ) ++ sinks.map(s => s.identifier -> SinkType)

    val grouped = allOutputs.groupBy { case (id, _) => id }

    grouped(identifier).toList match {
      case (_, firstType) :: (_, secondType) :: _ =>
        // Sort the types in lexicographic order to ensure consistent error messages.
        val sortedTypes = Seq(firstType.toString, secondType.toString).sorted
        throw new AnalysisException(
          errorClass = "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT",
          messageParameters = Map(
            "outputName" -> identifier.quotedString,
            "outputType1" -> sortedTypes.head,
            "outputType2" -> sortedTypes.last
          )
        )
      case _ => // No duplicates found.
    }
  }

  /**
   * Throws an exception if the given flow's identifier is used by multiple flows.
   *
   * @param flow The flow to check.
   * @param datasetType The type of dataset the flow writes to.
   * @param flows All flows in the graph.
   * @throws AnalysisException If the flow's identifier is used by multiple flows.
   */
  private def assertFlowIdentifierIsUnique(
      flow: UnresolvedFlow,
      flows: Seq[UnresolvedFlow]): Unit = {
    flows
      .groupBy(i => i.identifier)
      .get(flow.identifier)
      .filter(_.size > 1)
      .foreach { duplicateFlows =>
        val duplicateFlow = duplicateFlows.filter(_ != flow).head
        throw new AnalysisException(
          errorClass = "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW",
          messageParameters = Map(
            "flowName" -> flow.identifier.unquotedString,
            "datasetNames" -> Set(
              flow.destinationIdentifier.quotedString,
              duplicateFlow.destinationIdentifier.quotedString
            ).mkString(",")
          )
        )
    }
  }
}

object GraphRegistrationContext {
  sealed trait OutputType

  private object TableType extends OutputType {
    override def toString: String = "TABLE"
  }

  private object ViewType extends OutputType {
    override def toString: String = "VIEW"
  }

  private object SinkType extends OutputType {
    override def toString: String = "SINK"
  }
}
