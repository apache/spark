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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.{CatalystQl, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf, SimpleParserConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}

private[sql] class SparkQl(conf: ParserConf = SimpleParserConf()) extends CatalystQl(conf) {
  /** Check if a command should not be explained. */
  protected def isNoExplainCommand(command: String): Boolean = "TOK_DESCTABLE" == command

  protected override def nodeToPlan(node: ASTNode): LogicalPlan = {
    node match {
      // Just fake explain for any of the native commands.
      case Token("TOK_EXPLAIN", explainArgs) if isNoExplainCommand(explainArgs.head.text) =>
        ExplainCommand(OneRowRelation)

      case Token("TOK_EXPLAIN", explainArgs) if "TOK_CREATETABLE" == explainArgs.head.text =>
        val Some(crtTbl) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_CREATETABLE", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(crtTbl), extended = extended.isDefined)

      case Token("TOK_EXPLAIN", explainArgs) =>
        // Ignore FORMATTED if present.
        val Some(query) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_QUERY", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(query), extended = extended.isDefined)

      case Token("TOK_DESCTABLE", describeArgs) =>
        // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        val Some(tableType) :: formatted :: extended :: pretty :: Nil =
          getClauses(Seq("TOK_TABTYPE", "FORMATTED", "EXTENDED", "PRETTY"), describeArgs)
        if (formatted.isDefined || pretty.isDefined) {
          // FORMATTED and PRETTY are not supported and this statement will be treated as
          // a Hive native command.
          nodeToDescribeFallback(node)
        } else {
          tableType match {
            case Token("TOK_TABTYPE", Token("TOK_TABNAME", nameParts :: Nil) :: Nil) =>
              nameParts match {
                case Token(".", dbName :: tableName :: Nil) =>
                  // It is describing a table with the format like "describe db.table".
                  // TODO: Actually, a user may mean tableName.columnName. Need to resolve this
                  // issue.
                  val tableIdent = extractTableIdent(nameParts)
                  datasources.DescribeCommand(
                    UnresolvedRelation(tableIdent, None), isExtended = extended.isDefined)
                case Token(".", dbName :: tableName :: colName :: Nil) =>
                  // It is describing a column with the format like "describe db.table column".
                  nodeToDescribeFallback(node)
                case tableName =>
                  // It is describing a table with the format like "describe table".
                  datasources.DescribeCommand(
                    UnresolvedRelation(TableIdentifier(tableName.text), None),
                    isExtended = extended.isDefined)
              }
            // All other cases.
            case _ => nodeToDescribeFallback(node)
          }
        }

      case _ =>
        super.nodeToPlan(node)
    }
  }

  protected def nodeToDescribeFallback(node: ASTNode): LogicalPlan = noParseRule("Describe", node)
}
