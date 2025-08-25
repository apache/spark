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
 * @param storageRoot The root storage location for pipeline metadata, including checkpoints for
 *                    some kinds of flows.
 */
class GraphRegistrationContext(
    val defaultCatalog: String,
    val defaultDatabase: String,
    val defaultSqlConf: Map[String, String],
    val storageRoot: Option[String]) {
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

  def registerFlow(flowDef: UnresolvedFlow): Unit = {
    flows += flowDef.copy(sqlConf = defaultSqlConf ++ flowDef.sqlConf)
  }

  def toDataflowGraph: DataflowGraph = {
    if (tables.isEmpty && views.collect {
        case v: PersistedView =>
          v
      }.isEmpty && sinks.isEmpty) {
      throw new AnalysisException(errorClass = "RUN_EMPTY_PIPELINE", messageParameters = Map.empty)
    }
    val qualifiedTables = tables.toSeq.map { t =>
      t.copy(
        identifier = GraphIdentifierManager
          .parseAndQualifyTableIdentifier(
            rawTableIdentifier = t.identifier,
            currentCatalog = Some(defaultCatalog),
            currentDatabase = Some(defaultDatabase)
          )
          .identifier
      )
    }

    val validatedViews = views.toSeq.collect {
      case v: TemporaryView =>
        v.copy(
          identifier = GraphIdentifierManager
            .parseAndValidateTemporaryViewIdentifier(
              rawViewIdentifier = v.identifier
            )
        )
      case v: PersistedView =>
        v.copy(
          identifier = GraphIdentifierManager
            .parseAndValidatePersistedViewIdentifier(
              rawViewIdentifier = v.identifier,
              currentCatalog = Some(defaultCatalog),
              currentDatabase = Some(defaultDatabase)
            )
        )
    }

    val validatedSinks = sinks.toSeq.collect {
      case s: SinkImpl =>
        s.copy(
          identifier = GraphIdentifierManager
            .parseAndValidateSinkIdentifier(
              rawSinkIdentifier = s.identifier
            )
        )
    }

    val qualifiedFlows = flows.toSeq.map { f =>
      val isImplicitFlow = f.identifier == f.destinationIdentifier
      val flowWritesToView =
        validatedViews
          .filter(_.isInstanceOf[TemporaryView])
          .exists(_.identifier == f.destinationIdentifier)
      val flowWritesToSink =
        validatedSinks
          .filter(_.isInstanceOf[Sink])
          .exists(_.identifier == f.destinationIdentifier)

      // If the flow is created implicitly as part of defining a view, then we do not
      // qualify the flow identifier and the flow destination. This is because views are
      // not permitted to have multipart
      if ((isImplicitFlow && flowWritesToView) || flowWritesToSink) {
        f
      } else {
        f.copy(
          identifier = GraphIdentifierManager
            .parseAndQualifyFlowIdentifier(
              rawFlowIdentifier = f.identifier,
              currentCatalog = Some(defaultCatalog),
              currentDatabase = Some(defaultDatabase)
            )
            .identifier,
          destinationIdentifier = GraphIdentifierManager
            .parseAndQualifyFlowIdentifier(
              rawFlowIdentifier = f.destinationIdentifier,
              currentCatalog = Some(defaultCatalog),
              currentDatabase = Some(defaultDatabase)
            )
            .identifier
        )
      }
    }

    assertNoDuplicates(
      qualifiedTables = qualifiedTables,
      validatedViews = validatedViews,
      validatedSinks = validatedSinks,
      qualifiedFlows = qualifiedFlows
    )

    new DataflowGraph(
      tables = qualifiedTables,
      views = validatedViews,
      sinks = validatedSinks,
      flows = qualifiedFlows
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
        outputType = TableType,
        flows = qualifiedFlows
      )
    }
  }

  private def assertOutputIdentifierIsUnique(
      identifier: TableIdentifier,
      tables: Seq[Table],
      views: Seq[View],
      sinks: Seq[Sink]): Unit = {

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
          errorClass = "PIPELINE_DUPLICATE_IDENTIFIERS.DATASET",
          messageParameters = Map(
            "datasetName" -> identifier.quotedString,
            "datasetType1" -> sortedTypes.head,
            "datasetType2" -> sortedTypes.last
          )
        )
      case _ => // No duplicates found.
    }
  }

  private def assertFlowIdentifierIsUnique(
      flow: UnresolvedFlow,
      outputType: OutputType,
      flows: Seq[UnresolvedFlow]): Unit = {
    flows.groupBy(i => i.identifier).get(flow.identifier).filter(_.size > 1).foreach {
      duplicateFlows =>
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
