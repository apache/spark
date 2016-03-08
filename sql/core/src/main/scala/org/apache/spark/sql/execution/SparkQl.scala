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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

private[sql] class SparkQl(conf: ParserConf = SimpleParserConf()) extends CatalystQl(conf) {
  import ParserUtils._

  /** Check if a command should not be explained. */
  protected def isNoExplainCommand(command: String): Boolean = {
    "TOK_DESCTABLE" == command || "TOK_ALTERTABLE" == command
  }

  protected def extractProps(
      node: ASTNode,
      firstLevelProp: String,
      secondLevelProp: String): Seq[(String, String)] = {
    node match {
      case Token(x, options) if x == firstLevelProp =>
        options.map {
          case Token(y, keysAndValue) if y == secondLevelProp =>
            val key = keysAndValue.init.map(x => unquoteString(x.text)).mkString(".")
            val value = unquoteString(keysAndValue.last.text)
            (key, value)
          case _ =>
            throw new AnalysisException(s"Expected property '$secondLevelProp' in '${node.text}'")
        }
      case _ =>
        throw new AnalysisException(s"Expected property '$firstLevelProp' in '${node.text}'")
    }
  }

  protected override def nodeToPlan(node: ASTNode): LogicalPlan = {
    node match {
      case Token("TOK_SETCONFIG", Nil) =>
        val keyValueSeparatorIndex = node.remainder.indexOf('=')
        if (keyValueSeparatorIndex >= 0) {
          val key = node.remainder.substring(0, keyValueSeparatorIndex).trim
          val value = node.remainder.substring(keyValueSeparatorIndex + 1).trim
          SetCommand(Some(key -> Option(value)))
        } else if (node.remainder.nonEmpty) {
          SetCommand(Some(node.remainder -> None))
        } else {
          SetCommand(None)
        }

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

      case Token("TOK_REFRESHTABLE", nameParts :: Nil) =>
        val tableIdent = extractTableIdent(nameParts)
        RefreshTable(tableIdent)

      case Token("TOK_CREATEDATABASE", Token(databaseName, Nil) :: createDatabaseArgs) =>
        val Seq(
          allowExisting,
          dbLocation,
          databaseComment,
          dbprops) = getClauses(Seq(
          "TOK_IFNOTEXISTS",
          "TOK_DATABASELOCATION",
          "TOK_DATABASECOMMENT",
          "TOK_DATABASEPROPERTIES"), createDatabaseArgs)
        val location = dbLocation.map {
          case Token("TOK_DATABASELOCATION", Token(loc, Nil) :: Nil) => unquoteString(loc)
        }
        val comment = databaseComment.map {
          case Token("TOK_DATABASECOMMENT", Token(comment, Nil) :: Nil) => unquoteString(comment)
        }
        val props: Map[String, String] = dbprops.toSeq.flatMap {
          case Token("TOK_DATABASEPROPERTIES", propList) =>
            propList.flatMap(extractProps(_, "TOK_DBPROPLIST", "TOK_TABLEPROPERTY"))
        }.toMap
        CreateDatabase(databaseName, allowExisting.isDefined, location, comment, props)(node.source)

      case Token("TOK_CREATEFUNCTION", func :: as :: createFuncArgs) =>
        val funcName = func.map(x => unquoteString(x.text)).mkString(".")
        val asName = unquoteString(as.text)
        val Seq(
          rList,
          temp) = getClauses(Seq(
          "TOK_RESOURCE_LIST",
          "TOK_TEMPORARY"), createFuncArgs)
        val resourcesMap: Map[String, String] = rList.toSeq.flatMap {
          case Token("TOK_RESOURCE_LIST", resources) =>
            resources.map {
              case Token("TOK_RESOURCE_URI", rType :: Token(rPath, Nil) :: Nil) =>
                val resourceType = rType match {
                  case Token("TOK_JAR", Nil) => "jar"
                  case Token("TOK_FILE", Nil) => "file"
                  case Token("TOK_ARCHIVE", Nil) => "archive"
                }
                (resourceType, unquoteString(rPath))
            }
        }.toMap
        CreateFunction(funcName, asName, resourcesMap, temp.isDefined)(node.source)

      case Token("TOK_ALTERTABLE", alterTableArgs) =>
        AlterTableCommandParser.parse(node)

      case Token("TOK_CREATETABLEUSING", createTableArgs) =>
        val Seq(
          temp,
          allowExisting,
          Some(tabName),
          tableCols,
          Some(Token("TOK_TABLEPROVIDER", providerNameParts)),
          tableOpts,
          tableAs) = getClauses(Seq(
          "TEMPORARY",
          "TOK_IFNOTEXISTS",
          "TOK_TABNAME", "TOK_TABCOLLIST",
          "TOK_TABLEPROVIDER",
          "TOK_TABLEOPTIONS",
          "TOK_QUERY"), createTableArgs)

        val tableIdent: TableIdentifier = extractTableIdent(tabName)

        val columns = tableCols.map {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }

        val provider = providerNameParts.map {
          case Token(name, Nil) => name
        }.mkString(".")

        val options: Map[String, String] =
          tableOpts.toSeq.flatMap(extractProps(_, "TOK_TABLEOPTIONS", "TOK_TABLEOPTION")).toMap
        val asClause = tableAs.map(nodeToPlan)

        if (temp.isDefined && allowExisting.isDefined) {
          throw new AnalysisException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        if (asClause.isDefined) {
          if (columns.isDefined) {
            throw new AnalysisException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }

          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          CreateTableUsingAsSelect(tableIdent,
            provider,
            temp.isDefined,
            Array.empty[String],
            bucketSpec = None,
            mode,
            options,
            asClause.get)
        } else {
          CreateTableUsing(
            tableIdent,
            columns,
            provider,
            temp.isDefined,
            options,
            allowExisting.isDefined,
            managedIfNoPath = false)
        }

      case Token("TOK_SWITCHDATABASE", Token(database, Nil) :: Nil) =>
        SetDatabaseCommand(cleanIdentifier(database))

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
            case Token("TOK_TABTYPE", Token("TOK_TABNAME", nameParts) :: Nil) =>
              nameParts match {
                case Token(dbName, Nil) :: Token(tableName, Nil) :: Nil =>
                  // It is describing a table with the format like "describe db.table".
                  // TODO: Actually, a user may mean tableName.columnName. Need to resolve this
                  // issue.
                  val tableIdent = TableIdentifier(
                    cleanIdentifier(tableName), Some(cleanIdentifier(dbName)))
                  datasources.DescribeCommand(
                    UnresolvedRelation(tableIdent, None), isExtended = extended.isDefined)
                case Token(dbName, Nil) :: Token(tableName, Nil) :: Token(colName, Nil) :: Nil =>
                  // It is describing a column with the format like "describe db.table column".
                  nodeToDescribeFallback(node)
                case tableName :: Nil =>
                  // It is describing a table with the format like "describe table".
                  datasources.DescribeCommand(
                    UnresolvedRelation(TableIdentifier(cleanIdentifier(tableName.text)), None),
                    isExtended = extended.isDefined)
                case _ =>
                  nodeToDescribeFallback(node)
              }
            // All other cases.
            case _ =>
              nodeToDescribeFallback(node)
          }
        }

      case Token("TOK_CACHETABLE", Token(tableName, Nil) :: args) =>
       val Seq(lzy, selectAst) = getClauses(Seq("LAZY", "TOK_QUERY"), args)
        CacheTableCommand(tableName, selectAst.map(nodeToPlan), lzy.isDefined)

      case Token("TOK_UNCACHETABLE", Token(tableName, Nil) :: Nil) =>
        UncacheTableCommand(tableName)

      case Token("TOK_CLEARCACHE", Nil) =>
        ClearCacheCommand

      case Token("TOK_SHOWTABLES", args) =>
        val databaseName = args match {
          case Nil => None
          case Token("TOK_FROM", Token(dbName, Nil) :: Nil) :: Nil => Option(dbName)
          case _ => noParseRule("SHOW TABLES", node)
        }
        ShowTablesCommand(databaseName)

      case _ =>
        super.nodeToPlan(node)
    }
  }

  protected def nodeToDescribeFallback(node: ASTNode): LogicalPlan = noParseRule("Describe", node)
}
