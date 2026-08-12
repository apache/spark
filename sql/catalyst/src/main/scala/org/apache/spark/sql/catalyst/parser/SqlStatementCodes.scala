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

import org.apache.spark.sql.catalyst.analysis.UnresolvedExecuteImmediate
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Classification of a parsed SQL statement using ISO/IEC 9075-2:2023 Table 39,
 * "SQL-statement codes" (clause 23.1 &lt;get diagnostics statement&gt;).
 *
 * @param statementType BNF production name without angle brackets
 * @param statementIdentifier Table 39 Identifier column (or Spark product name)
 * @param statementCode Table 39 Code column; Spark-only statements use negative
 *                      implementation-defined codes (Table 39 IE005 / IV190)
 * @param statementClass Clause 4.41.2 function class, or
 *                       "implementation-defined statement" for Spark extensions
 * @param asSubquery True when &lt;table definition&gt; contains an &lt;as subquery clause&gt;
 */
case class SqlStatementClassification(
    statementType: String,
    statementIdentifier: String,
    statementCode: Int,
    statementClass: String,
    asSubquery: Boolean = false)

/**
 * Maps unresolved [[LogicalPlan]]s to Table 39 statement codes.
 *
 * Spark-only statements use the standard's implementation-defined escape hatch:
 * a product-specific identifier and a distinct negative code. Codes are
 * append-only and must never be renumbered.
 */
object SqlStatementCodes {

  // Standard Table 39 entries used by Spark SQL (ISO/IEC 9075-2:2023).
  val Select: SqlStatementClassification = SqlStatementClassification(
    "direct select statement: multiple rows", "SELECT", 21, "SQL-data statement")
  val Insert: SqlStatementClassification = SqlStatementClassification(
    "insert statement", "INSERT", 50, "SQL-data change statement")
  val DeleteWhere: SqlStatementClassification = SqlStatementClassification(
    "delete statement: searched", "DELETE WHERE", 19, "SQL-data change statement")
  val UpdateWhere: SqlStatementClassification = SqlStatementClassification(
    "update statement: searched", "UPDATE WHERE", 82, "SQL-data change statement")
  val Merge: SqlStatementClassification = SqlStatementClassification(
    "merge statement", "MERGE", 128, "SQL-data change statement")
  val CreateTable: SqlStatementClassification = SqlStatementClassification(
    "table definition", "CREATE TABLE", 77, "SQL-schema statement")
  val CreateView: SqlStatementClassification = SqlStatementClassification(
    "view definition", "CREATE VIEW", 84, "SQL-schema statement")
  val DropTable: SqlStatementClassification = SqlStatementClassification(
    "drop table statement", "DROP TABLE", 32, "SQL-schema statement")
  val DropView: SqlStatementClassification = SqlStatementClassification(
    "drop view statement", "DROP VIEW", 36, "SQL-schema statement")
  val AlterTable: SqlStatementClassification = SqlStatementClassification(
    "alter table statement", "ALTER TABLE", 4, "SQL-schema statement")
  val CreateSchema: SqlStatementClassification = SqlStatementClassification(
    "schema definition", "CREATE SCHEMA", 64, "SQL-schema statement")
  val DropSchema: SqlStatementClassification = SqlStatementClassification(
    "drop schema statement", "DROP SCHEMA", 31, "SQL-schema statement")
  val SetSchema: SqlStatementClassification = SqlStatementClassification(
    "set schema statement", "SET SCHEMA", 74, "SQL-session statement")
  val TruncateTable: SqlStatementClassification = SqlStatementClassification(
    "truncate table statement", "TRUNCATE TABLE", 139, "SQL-data change statement")
  val CreateRoutine: SqlStatementClassification = SqlStatementClassification(
    "schema routine", "CREATE ROUTINE", 14, "SQL-schema statement")
  val DropRoutine: SqlStatementClassification = SqlStatementClassification(
    "drop routine statement", "DROP ROUTINE", 30, "SQL-schema statement")
  val ExecuteImmediate: SqlStatementClassification = SqlStatementClassification(
    "execute immediate statement", "EXECUTE IMMEDIATE", 43, "SQL-dynamic statement")
  val Call: SqlStatementClassification = SqlStatementClassification(
    "call statement", "CALL", 7, "SQL-control statement")

  // Table 39 "Unrecognized statements": empty identifier, code 0.
  val Unrecognized: SqlStatementClassification = SqlStatementClassification(
    "", "", 0, "implementation-defined statement")

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

  private def spark(identifier: String, code: Int): SqlStatementClassification = {
    assert(code < 0, s"Spark statement codes must be negative, got $code")
    SqlStatementClassification(
      statementType = identifier.toLowerCase(java.util.Locale.ROOT),
      statementIdentifier = identifier,
      statementCode = code,
      statementClass = "implementation-defined statement")
  }

  /** Classify an unresolved logical plan. */
  def classify(plan: LogicalPlan): SqlStatementClassification = plan match {
    case UnresolvedWith(child, _, _) => classify(child)
    case _: CompoundBody => BeginEnd
    case _: InsertIntoStatement => Insert
    case _: DeleteFromTable | _: DeleteFromTableWithFilters => DeleteWhere
    case _: UpdateTable => UpdateWhere
    case _: MergeIntoTable => Merge
    case _: CreateTableAsSelect | _: ReplaceTableAsSelect =>
      CreateTable.copy(asSubquery = true)
    case _: CreateTable | _: CreateTableLike | _: ReplaceTable => CreateTable
    case _: CreateView => CreateView
    case _: DropTable => DropTable
    case _: DropView => DropView
    case _: CreateNamespace => CreateSchema
    case _: DropNamespace => DropSchema
    case _: SetCatalogAndNamespace => SetSchema
    case _: TruncateTable => TruncateTable
    case _: CreateFunction => CreateRoutine
    case _: DropFunction => DropRoutine
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
    case _: Command => Unrecognized
    case _ => Select
  }
}
