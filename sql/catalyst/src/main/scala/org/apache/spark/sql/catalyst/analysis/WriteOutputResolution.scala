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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.TableOutputResolver.DefaultValueFillMode._
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, V2WriteCommand}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn, StoredExpressionText}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.{SchemaAlignmentConfig, SupportsConfigurableSchemaAlignment}

/** Aligns a resolved write query with its target columns and stored generation expressions. */
private[sql] object WriteOutputResolution {

  /**
   * Resolves output columns after the table/query, schema changes and store policy are checked.
   * The callback analyzes newly constructed generated-column expressions under their stored
   * settings. Configuration is deferred to preserve the caller's receiver and read timing.
   */
  def resolveOutputRelation(
      v2Write: V2WriteCommand,
      analyze: LogicalPlan => LogicalPlan,
      conf: => SQLConf): V2WriteCommand = {
    TableOutputResolver.suitableForByNameCheck(v2Write.isByName,
      expected = v2Write.table.output, queryOutput = v2Write.query.output)
    // With schema evolution + coercion flag, missing top-level columns AND missing nested
    // struct fields are filled with defaults/null (RECURSE mode). Otherwise, only missing
    // top-level columns are filled via FILL mode; missing nested struct fields still cause
    // schema enforcement errors.
    val defaultValueFillMode =
      if (conf.coerceInsertNestedTypes && v2Write.schemaEvolutionEnabled) RECURSE
      else FILL
    // Generation expressions live on the table's columns, so attach them to the expected
    // output for TableOutputResolver, which auto-fills the generated columns the query is
    // missing.
    val expected = v2Write.table match {
      case r: DataSourceV2Relation => GeneratedColumn.attachGenerationExpressions(r)
      case _ => v2Write.table.output
    }
    val schemaAlignmentConfig = v2Write.table match {
      case ExtractV2Table(table: SupportsConfigurableSchemaAlignment) =>
        table.schemaAlignmentConfig()
      case _ => SchemaAlignmentConfig.DEFAULT
    }
    def resolveOutput(readGenerationExpr: String => Expression) =
      TableOutputResolver.resolveOutputColumnsWithGeneratedInfo(
        v2Write.table.name, expected, v2Write.query, v2Write.isByName, conf,
        defaultValueFillMode, schemaAlignmentConfig.ansiStoreAssignmentCastCheck(),
        readGenerationExpr)
    val (projection, autoFilledGenCols) = resolveOutput(StoredExpressionText.parse)
    if (projection != v2Write.query) {
      val cleanedTable = v2Write.table match {
        case r: DataSourceV2Relation =>
          val cleaned = r.output.map(CharVarcharUtils.cleanAttrMetadata)
          val cleanedRelation = r.copy(output =
            GeneratedColumn.markAutoFilledGeneratedColumns(cleaned, autoFilledGenCols))
          cleanedRelation.copyTagsFrom(r)
          cleanedRelation
        case other => other
      }
      v2Write
        .withNewQuery(analyzeAutoFilledGeneratedColumns(
          projection,
          autoFilledGenCols,
          resolveOutput(StoredExpressionText.parseWithSessionAnsi)._1,
          analyze))
        .withNewTable(cleanedTable)
    } else {
      v2Write
    }
  }

  /**
   * Analyzes the auto-filled generated columns of a write under the settings their stored text
   * is read under, so that the implicit casts and the functions of a generation expression do
   * not follow the session that writes.
   *
   * A column these settings cannot resolve comes from `readInSession`, where its text is read
   * with the ANSI value of the session, and the analysis of the write resolves it under the
   * session. So does a text a catalog stores that analyzes to another plan.
   */
  private def analyzeAutoFilledGeneratedColumns(
      projection: LogicalPlan,
      autoFilled: Set[String],
      readInSession: => LogicalPlan,
      analyze: LogicalPlan => LogicalPlan): LogicalPlan = projection match {
    case p @ Project(projectList, child) if autoFilled.nonEmpty =>
      lazy val sessionCols = readInSession.expressions.collect {
        case c: NamedExpression => c.name -> c
      }.toMap
      p.copy(projectList = projectList.map {
        case c if autoFilled.contains(c.name) =>
          StoredExpressionText.analyze(c, sessionCols(c.name), child.output, analyze)
        case c => c
      })
    case other => other
  }
}
