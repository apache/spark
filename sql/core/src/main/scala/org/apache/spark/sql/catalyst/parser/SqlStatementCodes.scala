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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.analysis.{
  RelationTimeTravel,
  ResolvedInlineTable,
  UnresolvedExecuteImmediate,
  UnresolvedHaving,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedTableValuedFunction
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, RefreshResource}
import org.apache.spark.sql.metricview.logical.CreateMetricView

/**
 * Classification of a parsed SQL statement using ISO/IEC 9075-2:2023 Table 39,
 * "SQL-statement codes" (clause 23.1 &lt;get diagnostics statement&gt;).
 *
 * @param statementIdentifier Table 39 Identifier column (or Spark product name)
 * @param statementCode Table 39 Code column; Spark-only statements use negative
 *                      implementation-defined codes (Table 39 IE005 / IV190)
 */
case class SqlStatementClassification(
    statementIdentifier: String,
    statementCode: Int)

/**
 * Maps unresolved [[LogicalPlan]]s to Table 39 statement codes.
 *
 * Spark-only statements use the standard's implementation-defined escape hatch:
 * a product-specific identifier and a distinct negative code. Codes are
 * append-only and must never be renumbered.
 *
 * Unknown plans map to [[Unrecognized]] (empty identifier, code 0). Query
 * shapes are allowlisted; unknown non-commands are not assumed to be SELECT.
 */
object SqlStatementCodes {

  // Standard Table 39 entries used by Spark SQL (ISO/IEC 9075-2:2023).
  val Select: SqlStatementClassification = SqlStatementClassification("SELECT", 21)
  val Insert: SqlStatementClassification = SqlStatementClassification("INSERT", 50)
  val DeleteWhere: SqlStatementClassification = SqlStatementClassification("DELETE WHERE", 19)
  val UpdateWhere: SqlStatementClassification = SqlStatementClassification("UPDATE WHERE", 82)
  val Merge: SqlStatementClassification = SqlStatementClassification("MERGE", 128)
  val CreateTable: SqlStatementClassification = SqlStatementClassification("CREATE TABLE", 77)
  val CreateView: SqlStatementClassification = SqlStatementClassification("CREATE VIEW", 84)
  val DropTable: SqlStatementClassification = SqlStatementClassification("DROP TABLE", 32)
  val DropView: SqlStatementClassification = SqlStatementClassification("DROP VIEW", 36)
  val AlterTable: SqlStatementClassification = SqlStatementClassification("ALTER TABLE", 4)
  val CreateSchema: SqlStatementClassification = SqlStatementClassification("CREATE SCHEMA", 64)
  val DropSchema: SqlStatementClassification = SqlStatementClassification("DROP SCHEMA", 31)
  val SetSchema: SqlStatementClassification = SqlStatementClassification("SET SCHEMA", 74)
  val TruncateTable: SqlStatementClassification =
    SqlStatementClassification("TRUNCATE TABLE", 139)
  val CreateRoutine: SqlStatementClassification = SqlStatementClassification("CREATE ROUTINE", 14)
  val DropRoutine: SqlStatementClassification = SqlStatementClassification("DROP ROUTINE", 30)
  val ExecuteImmediate: SqlStatementClassification =
    SqlStatementClassification("EXECUTE IMMEDIATE", 43)
  val Call: SqlStatementClassification = SqlStatementClassification("CALL", 7)

  // Table 39 "Unrecognized statements": empty identifier, code 0.
  val Unrecognized: SqlStatementClassification = SqlStatementClassification("", 0)

  // Spark product-specific identifiers with append-only negative codes
  // (Table 39 implementation-defined / IE005 row: negative Code values).
  val CacheTable: SqlStatementClassification = spark("CACHE TABLE", -1)
  val CacheTableAsSelect: SqlStatementClassification = spark("CACHE TABLE AS SELECT", -2)
  val UncacheTable: SqlStatementClassification = spark("UNCACHE TABLE", -3)
  val RefreshTable: SqlStatementClassification = spark("REFRESH TABLE", -4)
  val ShowTables: SqlStatementClassification = spark("SHOW TABLES", -5)
  val DescribeTable: SqlStatementClassification = spark("DESCRIBE TABLE", -6)
  val AnalyzeTable: SqlStatementClassification = spark("ANALYZE TABLE", -7)
  val DeclareVariable: SqlStatementClassification = spark("DECLARE VARIABLE", -8)
  val SetVariable: SqlStatementClassification = spark("SET VARIABLE", -9)
  val DropVariable: SqlStatementClassification = spark("DROP VARIABLE", -10)
  val ShowTableProperties: SqlStatementClassification = spark("SHOW TBLPROPERTIES", -11)
  val DescribeNamespace: SqlStatementClassification = spark("DESCRIBE NAMESPACE", -12)
  val ShowFunctions: SqlStatementClassification = spark("SHOW FUNCTIONS", -13)
  val DescribeFunction: SqlStatementClassification = spark("DESCRIBE FUNCTION", -14)
  val ShowCreateTable: SqlStatementClassification = spark("SHOW CREATE TABLE", -15)
  val ShowColumns: SqlStatementClassification = spark("SHOW COLUMNS", -16)
  val ShowPartitions: SqlStatementClassification = spark("SHOW PARTITIONS", -17)
  val ShowViews: SqlStatementClassification = spark("SHOW VIEWS", -18)
  val RefreshFunction: SqlStatementClassification = spark("REFRESH FUNCTION", -19)
  val CommentOnNamespace: SqlStatementClassification = spark("COMMENT ON NAMESPACE", -20)
  val CommentOnTable: SqlStatementClassification = spark("COMMENT ON TABLE", -21)
  // SQL/PSM-style scripting (9075-4); not in Foundation Table 39.
  val BeginEnd: SqlStatementClassification = spark("BEGIN END", -22)
  // SparkSqlParser-only session / resource commands (append-only).
  val Explain: SqlStatementClassification = spark("EXPLAIN", -23)
  val Set: SqlStatementClassification = spark("SET", -24)
  val Reset: SqlStatementClassification = spark("RESET", -25)
  val AddJar: SqlStatementClassification = spark("ADD JAR", -26)
  val AddFile: SqlStatementClassification = spark("ADD FILE", -27)
  val AddArchive: SqlStatementClassification = spark("ADD ARCHIVE", -28)
  val ListJar: SqlStatementClassification = spark("LIST JAR", -29)
  val ListFile: SqlStatementClassification = spark("LIST FILE", -30)
  val ClearCache: SqlStatementClassification = spark("CLEAR CACHE", -31)
  val RefreshResourceCmd: SqlStatementClassification = spark("REFRESH RESOURCE", -32)
  val DescribeQuery: SqlStatementClassification = spark("DESCRIBE QUERY", -33)
  val ShowCatalogs: SqlStatementClassification = spark("SHOW CATALOGS", -34)
  val ShowCurrentNamespace: SqlStatementClassification =
    spark("SHOW CURRENT NAMESPACE", -35)
  val SetCatalog: SqlStatementClassification = spark("SET CATALOG", -36)
  val CreateMetricViewStmt: SqlStatementClassification = spark("CREATE METRIC VIEW", -37)

  private def spark(identifier: String, code: Int): SqlStatementClassification = {
    assert(code < 0, s"Spark statement codes must be negative, got $code")
    SqlStatementClassification(statementIdentifier = identifier, statementCode = code)
  }

  /** Classify an unresolved logical plan. */
  def classify(plan: LogicalPlan): SqlStatementClassification = plan match {
    case UnresolvedWith(child, _, _) => classify(child)
    case _: CompoundBody => BeginEnd
    case _: InsertIntoStatement => Insert
    case _: DeleteFromTable | _: DeleteFromTableWithFilters => DeleteWhere
    case _: UpdateTable => UpdateWhere
    case _: MergeIntoTable => Merge
    case _: CreateTableAsSelect | _: ReplaceTableAsSelect => CreateTable
    case _: CreateTable | _: CreateTableLike | _: ReplaceTable => CreateTable
    case _: CreateView | _: CreateViewCommand | _: CreateTempViewUsing => CreateView
    case _: DropTable => DropTable
    case _: DropView => DropView
    case _: CreateNamespace => CreateSchema
    case _: DropNamespace => DropSchema
    case _: SetCatalogAndNamespace | _: SetNamespaceCommand => SetSchema
    case _: SetCatalogCommand => SetCatalog
    case _: TruncateTable => TruncateTable
    case _: CreateFunction | _: CreateFunctionCommand |
         _: CreateUserDefinedFunction | _: CreateUserDefinedFunctionCommand =>
      CreateRoutine
    case _: DropFunction | _: DropFunctionCommand => DropRoutine
    case _: UnresolvedExecuteImmediate => ExecuteImmediate
    case _: Call => Call
    case _: CommentOnTable => CommentOnTable
    case _: AlterTableCommand | _: RenameTable => AlterTable
    case _: CacheTable => CacheTable
    case _: CacheTableAsSelect => CacheTableAsSelect
    case _: UncacheTable => UncacheTable
    case _: RefreshTable => RefreshTable
    case _: ShowTables | _: ShowTablesExtended => ShowTables
    case _: DescribeRelation | _: DescribeTablePartition | _: DescribeColumn =>
      DescribeTable
    case _: DescribeQueryCommand => DescribeQuery
    case _: AnalyzeTable | _: AnalyzeTables | _: AnalyzeColumn => AnalyzeTable
    case _: CreateVariable => DeclareVariable
    case _: SetVariable => SetVariable
    case _: DropVariable => DropVariable
    case _: ShowTableProperties => ShowTableProperties
    case _: DescribeNamespace => DescribeNamespace
    case _: ShowFunctions => ShowFunctions
    case _: DescribeFunction => DescribeFunction
    case _: ShowCreateTable => ShowCreateTable
    case _: ShowColumns => ShowColumns
    case _: ShowPartitions | _: ShowTablePartition => ShowPartitions
    case _: ShowViews => ShowViews
    case _: RefreshFunction => RefreshFunction
    case _: CommentOnNamespace => CommentOnNamespace
    case _: ExplainCommand => Explain
    case _: SetCommand => Set
    case _: ResetCommand => Reset
    case _: AddJarsCommand => AddJar
    case _: AddFilesCommand => AddFile
    case _: AddArchivesCommand => AddArchive
    case _: ListJarsCommand => ListJar
    case _: ListFilesCommand => ListFile
    case ClearCacheCommand => ClearCache
    case _: RefreshResource => RefreshResourceCmd
    case _: ShowCatalogsCommand => ShowCatalogs
    case _: ShowCurrentNamespaceCommand => ShowCurrentNamespace
    case _: CreateMetricView | _: CreateMetricViewCommand => CreateMetricViewStmt
    case _: Command => Unrecognized
    case p if isQueryPlan(p) => Select
    case _ => Unrecognized
  }

  /**
   * Allowlisted query-shaped plans. Unknown non-command plans are not assumed
   * to be SELECT.
   */
  private def isQueryPlan(plan: LogicalPlan): Boolean = plan match {
    case _: Project | _: Aggregate | _: Distinct | _: Filter | _: Sort |
         _: GlobalLimit | _: LocalLimit | _: Join | _: Union | _: Except |
         _: Intersect | _: SubqueryAlias | _: Repartition |
         _: RepartitionByExpression | _: Sample | _: Range |
         _: OneRowRelation | _: LocalRelation | _: Deduplicate |
         _: Expand | _: Generate | _: Window | _: Tail | _: Offset |
         _: LateralJoin | _: UnresolvedHaving | _: CollectMetrics |
         _: WithCTE | _: UnresolvedRelation | _: UnresolvedInlineTable |
         _: ResolvedInlineTable | _: RelationTimeTravel |
         _: UnresolvedTableValuedFunction => true
    case _ => false
  }
}
