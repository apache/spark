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

package org.apache.spark.sql.hive.thriftserver

import java.sql.{DatabaseMetaData, ResultSet, SQLFeatureNotSupportedException}

import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.cli.HiveSQLException

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.VersionUtils

class SparkMetadataOperationSuite extends HiveThriftServer2TestBase {

  override def mode: ServerMode.Value = ServerMode.binary

  test("Spark's own GetSchemasOperation(SparkGetSchemasOperation)") {
    def checkResult(rs: ResultSet, dbNames: Seq[String]): Unit = {
      val expected = dbNames.iterator
      while (rs.next() || expected.hasNext) {
        assert(rs.getString("TABLE_SCHEM") === expected.next())
        assert(rs.getString("TABLE_CATALOG").isEmpty)
      }
      // Make sure there are no more elements
      assert(!rs.next())
      assert(!expected.hasNext, "All expected schemas should be visited")
    }

    val dbs = Seq("db1", "db2", "db33", "db44")
    val dbDflts = Seq("default", "global_temp")
    withDatabase(dbs: _*) { statement =>
      dbs.foreach( db => statement.execute(s"CREATE DATABASE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq("", "%", null, ".*", "_*", "_%", ".%") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs ++ dbDflts)
      }

      Seq("db%", "db*") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs)
      }

      Seq("db_", "db.") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs.take(2))
      }

      checkResult(metaData.getSchemas(null, "db1"), Seq("db1"))
      checkResult(metaData.getSchemas(null, "db_not_exist"), Seq.empty)

      val e = intercept[HiveSQLException](metaData.getSchemas(null, "*"))
      assert(e.getCause.getMessage ===
        "Error operating GET_SCHEMAS Dangling meta character '*' near index 0\n*\n^")
    }
  }

  test("Spark's own GetTablesOperation(SparkGetTablesOperation)") {
    def checkResult(rs: ResultSet, tableNames: Seq[String]): Unit = {
      for (i <- tableNames.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_NAME") === tableNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT, val STRING)",
        "CREATE TABLE table2(key INT, val STRING)",
        "CREATE VIEW view1 AS SELECT * FROM table2",
        "CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_global_temp_1 AS SELECT 1 AS col1",
        "CREATE OR REPLACE TEMPORARY VIEW view_temp_1 AS SELECT 1 as col1"
      ).foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getTables(null, "%", "%", null),
        Seq("table1", "table2", "view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "table1", null), Seq("table1"))

      checkResult(metaData.getTables(null, "%", "table_not_exist", null), Seq.empty)

      checkResult(metaData.getTables(null, "%", "%", Array("TABLE")),
        Seq("table1", "table2"))

      checkResult(metaData.getTables(null, "%", "%", Array("VIEW")),
        Seq("view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "view_global_temp_1", null),
        Seq("view_global_temp_1"))

      checkResult(metaData.getTables(null, "%", "view_temp_1", null),
        Seq("view_temp_1"))

      checkResult(metaData.getTables(null, "%", "%", Array("TABLE", "VIEW")),
        Seq("table1", "table2", "view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "table_not_exist", Array("TABLE", "VIEW")),
        Seq.empty)
    }
  }

  test("Spark's own GetColumnsOperation(SparkGetColumnsOperation)") {
    def checkResult(
        rs: ResultSet,
        columns: Seq[(String, String, String, String, String)]) : Unit = {
      for (i <- columns.indices) {
        assert(rs.next())
        val col = columns(i)
        assert(rs.getString("TABLE_NAME") === col._1)
        assert(rs.getString("COLUMN_NAME") === col._2)
        assert(rs.getString("DATA_TYPE") === col._3)
        assert(rs.getString("TYPE_NAME") === col._4)
        assert(rs.getString("REMARKS") === col._5)
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT comment 'Int column', val STRING comment 'String column')",
        "CREATE TABLE table2(key INT, val DECIMAL comment 'Decimal column')",
        "CREATE VIEW view1 AS SELECT key FROM table1",
        "CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_global_temp_1 AS SELECT 2 AS col2",
        "CREATE OR REPLACE TEMPORARY VIEW view_temp_1 AS SELECT 2 as col2"
      ).foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getColumns(null, "%", "%", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column"),
          ("table2", "key", "4", "INT", ""),
          ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
          ("view1", "key", "4", "INT", "Int column"),
          ("view_global_temp_1", "col2", "4", "INT", ""),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "table1", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column")))

      checkResult(metaData.getColumns(null, "%", "table1", "key"),
        Seq(("table1", "key", "4", "INT", "Int column")))

      checkResult(metaData.getColumns(null, "%", "view%", null),
        Seq(
          ("view1", "key", "4", "INT", "Int column"),
          ("view_global_temp_1", "col2", "4", "INT", ""),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_global_temp_1", null),
        Seq(("view_global_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_temp_1", null),
        Seq(("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_temp_1", "col2"),
        Seq(("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "default", "%", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column"),
          ("table2", "key", "4", "INT", ""),
          ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
          ("view1", "key", "4", "INT", "Int column"),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "table_not_exist", null), Seq.empty)
    }
  }

  test("Spark's own GetTableTypesOperation(SparkGetTableTypesOperation)") {
    def checkResult(rs: ResultSet, tableTypes: Seq[String]): Unit = {
      for (i <- tableTypes.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_TYPE") === tableTypes(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTableTypes, Seq("TABLE", "VIEW"))
    }
  }

  test("Spark's own GetFunctionsOperation(SparkGetFunctionsOperation)") {
    def checkResult(rs: ResultSet, functionNames: Seq[String]): Unit = {
      functionNames.foreach { func =>
        val exprInfo = FunctionRegistry.expressions(func)._1
        assert(rs.next())
        assert(rs.getString("FUNCTION_SCHEM") === "default")
        assert(rs.getString("FUNCTION_NAME") === exprInfo.getName)
        assert(rs.getString("REMARKS") ===
          s"Usage: ${exprInfo.getUsage}\nExtended Usage:${exprInfo.getExtended}")
        assert(rs.getInt("FUNCTION_TYPE") === DatabaseMetaData.functionResultUnknown)
        assert(rs.getString("SPECIFIC_NAME") === exprInfo.getClassName)
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      // Hive does not have an overlay function, we use overlay to test.
      checkResult(metaData.getFunctions(null, null, "overlay"), Seq("overlay"))
      checkResult(metaData.getFunctions(null, null, "overla*"), Seq("overlay"))
      checkResult(metaData.getFunctions(null, "", "overla*"), Seq("overlay"))
      checkResult(metaData.getFunctions(null, null, "does-not-exist*"), Seq.empty)
      checkResult(metaData.getFunctions(null, "default", "overlay"), Seq("overlay"))
      checkResult(metaData.getFunctions(null, "default", "shift*"),
        Seq("shiftleft", "shiftright", "shiftrightunsigned"))
      checkResult(metaData.getFunctions(null, "default", "upPer"), Seq("upper"))
    }
  }

  test("Spark's own GetCatalogsOperation(SparkGetCatalogsOperation)") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val rs = metaData.getCatalogs
      assert(!rs.next())
    }
  }

  test("GetTypeInfo Thrift API") {
    def checkResult(rs: ResultSet, typeNames: Seq[String]): Unit = {
      for (i <- typeNames.indices) {
        assert(rs.next())
        assert(rs.getString("TYPE_NAME") === typeNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTypeInfo, SparkGetTypeInfoUtil.supportedType.map(_.getName))
    }
  }

  test("check results from get columns operation from thrift server") {
    val schemaName = "default"
    val tableName = "spark_get_col_operation"
    val schema = new StructType()
      .add("c0", "boolean", nullable = false, "0")
      .add("c1", "tinyint", nullable = true, "1")
      .add("c2", "smallint", nullable = false, "2")
      .add("c3", "int", nullable = true, "3")
      .add("c4", "long", nullable = false, "4")
      .add("c5", "float", nullable = true, "5")
      .add("c6", "double", nullable = false, "6")
      .add("c7", "decimal(38, 20)", nullable = true, "7")
      .add("c8", "decimal(10, 2)", nullable = false, "8")
      .add("c9", "string", nullable = true, "9")
      .add("c10", "array<long>", nullable = false, "10")
      .add("c11", "array<string>", nullable = true, "11")
      .add("c12", "map<smallint, tinyint>", nullable = false, "12")
      .add("c13", "date", nullable = true, "13")
      .add("c14", "timestamp", nullable = false, "14")
      .add("c15", "struct<X: bigint,Y: double>", nullable = true, "15")
      .add("c16", "binary", nullable = false, "16")
      .add("c17", "char(255)", nullable = true, "17")
      .add("c18", "varchar(1024)", nullable = false, "18")

    val ddl =
      s"""
         |CREATE TABLE $schemaName.$tableName (
         |  ${schema.toDDL}
         |)
         |using parquet""".stripMargin

    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val databaseMetaData = statement.getConnection.getMetaData
      val rowSet = databaseMetaData.getColumns("", schemaName, tableName, null)

      import java.sql.Types._
      val expectedJavaTypes = Seq(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
        DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY,
        CHAR, VARCHAR)

      var pos = 0

      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === schemaName)
        assert(rowSet.getString("TABLE_NAME") === tableName)
        assert(rowSet.getString("COLUMN_NAME") === schema(pos).name)
        assert(rowSet.getInt("DATA_TYPE") === expectedJavaTypes(pos))
        assert(rowSet.getString("TYPE_NAME") === schema(pos).dataType.sql)

        val colSize = rowSet.getInt("COLUMN_SIZE")
        schema(pos).dataType match {
          case StringType | BinaryType | _: ArrayType | _: MapType | _: VarcharType =>
            assert(colSize === 0)
          case o => assert(colSize === o.defaultSize)
        }

        assert(rowSet.getInt("BUFFER_LENGTH") === 0) // not used
        val decimalDigits = rowSet.getInt("DECIMAL_DIGITS")
        schema(pos).dataType match {
          case BooleanType | _: IntegerType => assert(decimalDigits === 0)
          case d: DecimalType => assert(decimalDigits === d.scale)
          case FloatType => assert(decimalDigits === 7)
          case DoubleType => assert(decimalDigits === 15)
          case TimestampType => assert(decimalDigits === 6)
          case _ => assert(decimalDigits === 0) // nulls
        }

        val radix = rowSet.getInt("NUM_PREC_RADIX")
        schema(pos).dataType match {
          case _: NumericType => assert(radix === 10)
          case _ => assert(radix === 0) // nulls
        }

        assert(rowSet.getInt("NULLABLE") === 1)
        assert(rowSet.getString("REMARKS") === pos.toString)
        assert(rowSet.getInt("ORDINAL_POSITION") === pos)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
        pos += 1
      }

      assert(pos === 19, "all columns should have been verified")
    }
  }

  test("get columns operation should handle interval column properly") {
    val viewName = "view_interval"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select interval 1 day as i"

    withJdbcStatement(viewName) { statement =>
      statement.execute(s"SET ${SQLConf.LEGACY_INTERVAL_ENABLED.key}=true")
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, null)
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName)
        assert(rowSet.getString("COLUMN_NAME") === "i")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.OTHER)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(CalendarIntervalType.sql))
        assert(rowSet.getInt("COLUMN_SIZE") === CalendarIntervalType.defaultSize)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 0)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }
  }

  test("SPARK-35085: Get columns operation should handle ANSI interval column properly") {
    val viewName1 = "view_interval1"
    val yearMonthDDL =
      s"CREATE GLOBAL TEMP VIEW $viewName1 as select interval '1-1' year to month as i"

    withJdbcStatement(viewName1) { statement =>
      statement.execute(yearMonthDDL)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName1, null)
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName1)
        assert(rowSet.getString("COLUMN_NAME") === "i")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.OTHER)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(YearMonthIntervalType().sql))
        assert(rowSet.getInt("COLUMN_SIZE") === YearMonthIntervalType().defaultSize)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 0)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }

    val viewName2 = "view_interval2"
    val dayTimeDDL =
      s"CREATE GLOBAL TEMP VIEW $viewName2 as select interval '1 2:3:4.001' day to second as i"

    withJdbcStatement(viewName2) { statement =>
      statement.execute(dayTimeDDL)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName2, null)
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName2)
        assert(rowSet.getString("COLUMN_NAME") === "i")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.OTHER)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(DayTimeIntervalType().sql))
        assert(rowSet.getInt("COLUMN_SIZE") === DayTimeIntervalType().defaultSize)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 0)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }
  }

  test("handling null in view for get columns operations") {
    val viewName = "view_null"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select null as n"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, "n")
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName)
        assert(rowSet.getString("COLUMN_NAME") === "n")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.NULL)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(NullType.sql))
        assert(rowSet.getInt("COLUMN_SIZE") === 1)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 1)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }
  }

  test("Hive ThriftServer JDBC Database MetaData API Auditing - Method not supported") {
    // These APIs belong to the upstream Apache Hive's hive-jdbc artifact where defines the hive
    // behavior. Users can also use it to interact with Spark ThriftServer directly. Some behaviors
    // are not fully consistent with Spark e.g. we support correlated subqueries but the hive-jdbc
    // now fail directly at client side. There is nothing we can do but accept the current
    // condition and highlight the difference and make it perspective in future changes both from
    // upstream and inside Spark.
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq(
        () => metaData.allProceduresAreCallable,
        () => metaData.getURL,
        () => metaData.getUserName,
        () => metaData.isReadOnly,
        () => metaData.nullsAreSortedHigh,
        () => metaData.nullsAreSortedLow,
        () => metaData.nullsAreSortedAtStart,
        () => metaData.nullsAreSortedAtEnd,
        () => metaData.usesLocalFiles,
        () => metaData.usesLocalFilePerTable,
        () => metaData.supportsMixedCaseIdentifiers,
        () => metaData.supportsMixedCaseQuotedIdentifiers,
        () => metaData.storesUpperCaseIdentifiers,
        () => metaData.storesUpperCaseQuotedIdentifiers,
        () => metaData.storesLowerCaseIdentifiers,
        () => metaData.storesLowerCaseQuotedIdentifiers,
        () => metaData.storesMixedCaseIdentifiers,
        () => metaData.storesMixedCaseQuotedIdentifiers,
        () => metaData.getSQLKeywords,
        () => metaData.nullPlusNonNullIsNull,
        () => metaData.supportsConvert,
        () => metaData.supportsTableCorrelationNames,
        () => metaData.supportsDifferentTableCorrelationNames,
        () => metaData.supportsExpressionsInOrderBy,
        () => metaData.supportsOrderByUnrelated,
        () => metaData.supportsGroupByUnrelated,
        () => metaData.supportsGroupByBeyondSelect,
        () => metaData.supportsLikeEscapeClause,
        () => metaData.supportsMultipleTransactions,
        () => metaData.supportsMinimumSQLGrammar,
        () => metaData.supportsCoreSQLGrammar,
        () => metaData.supportsExtendedSQLGrammar,
        () => metaData.supportsANSI92EntryLevelSQL,
        () => metaData.supportsANSI92IntermediateSQL,
        () => metaData.supportsANSI92FullSQL,
        () => metaData.supportsIntegrityEnhancementFacility,
        () => metaData.isCatalogAtStart,
        () => metaData.supportsSubqueriesInComparisons,
        () => metaData.supportsSubqueriesInExists,
        () => metaData.supportsSubqueriesInIns,
        () => metaData.supportsSubqueriesInQuantifieds,
        // Spark support this, see https://issues.apache.org/jira/browse/SPARK-18455
        () => metaData.supportsCorrelatedSubqueries,
        () => metaData.supportsOpenCursorsAcrossCommit,
        () => metaData.supportsOpenCursorsAcrossRollback,
        () => metaData.supportsOpenStatementsAcrossCommit,
        () => metaData.supportsOpenStatementsAcrossRollback,
        () => metaData.getMaxBinaryLiteralLength,
        () => metaData.getMaxCharLiteralLength,
        () => metaData.getMaxColumnsInGroupBy,
        () => metaData.getMaxColumnsInIndex,
        () => metaData.getMaxColumnsInOrderBy,
        () => metaData.getMaxColumnsInSelect,
        () => metaData.getMaxColumnsInTable,
        () => metaData.getMaxConnections,
        () => metaData.getMaxCursorNameLength,
        () => metaData.getMaxIndexLength,
        () => metaData.getMaxSchemaNameLength,
        () => metaData.getMaxProcedureNameLength,
        () => metaData.getMaxCatalogNameLength,
        () => metaData.getMaxRowSize,
        () => metaData.doesMaxRowSizeIncludeBlobs,
        () => metaData.getMaxStatementLength,
        () => metaData.getMaxStatements,
        () => metaData.getMaxTableNameLength,
        () => metaData.getMaxTablesInSelect,
        () => metaData.getMaxUserNameLength,
        () => metaData.supportsTransactionIsolationLevel(1),
        () => metaData.supportsDataDefinitionAndDataManipulationTransactions,
        () => metaData.supportsDataManipulationTransactionsOnly,
        () => metaData.dataDefinitionCausesTransactionCommit,
        () => metaData.dataDefinitionIgnoredInTransactions,
        () => metaData.getColumnPrivileges("", "%", "%", "%"),
        () => metaData.getTablePrivileges("", "%", "%"),
        () => metaData.getBestRowIdentifier("", "%", "%", 0, true),
        () => metaData.getVersionColumns("", "%", "%"),
        () => metaData.getExportedKeys("", "default", ""),
        () => metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, 2),
        () => metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.supportsNamedParameters,
        () => metaData.supportsMultipleOpenResults,
        () => metaData.supportsGetGeneratedKeys,
        () => metaData.getSuperTypes("", "%", "%"),
        () => metaData.getSuperTables("", "%", "%"),
        () => metaData.getAttributes("", "%", "%", "%"),
        () => metaData.getResultSetHoldability,
        () => metaData.locatorsUpdateCopy,
        () => metaData.supportsStatementPooling,
        () => metaData.getRowIdLifetime,
        () => metaData.supportsStoredFunctionsUsingCallSyntax,
        () => metaData.autoCommitFailureClosesAllResultSets,
        () => metaData.getClientInfoProperties,
        () => metaData.getFunctionColumns("", "%", "%", "%"),
        () => metaData.getPseudoColumns("", "%", "%", "%"),
        () => metaData.generatedKeyAlwaysReturned).foreach { func =>
        val e = intercept[SQLFeatureNotSupportedException](func())
        assert(e.getMessage === "Method not supported")
      }
    }
  }

  test("Hive ThriftServer JDBC Database MetaData API Auditing - Method supported") {
    // These APIs belong to the upstream Apache Hive's hive-jdbc artifact where defines the hive
    // behavior. Users can also use it to interact with Spark ThriftServer directly. Some behaviors
    // are not fully consistent with Spark e.g. we can work with multiple catalogs.
    // There is nothing we can do but accept the current condition and highlight the difference
    // and make it perspective in future changes both from upstream and inside Spark.
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      assert(metaData.allTablesAreSelectable)
      assert(metaData.getDatabaseProductName === "Spark SQL")
      assert(metaData.getDatabaseProductVersion === SPARK_VERSION)
      assert(metaData.getDriverName === "Hive JDBC")
      assert(metaData.getDriverVersion === HiveVersionInfo.getVersion)
      assert(metaData.getDatabaseMajorVersion === VersionUtils.majorVersion(SPARK_VERSION))
      assert(metaData.getDatabaseMinorVersion === VersionUtils.minorVersion(SPARK_VERSION))
      assert(metaData.getIdentifierQuoteString === " ",
        "This method returns a space \" \" if identifier quoting is not supported")
      assert(metaData.getNumericFunctions === "")
      assert(metaData.getStringFunctions === "")
      assert(metaData.getSystemFunctions === "")
      assert(metaData.getTimeDateFunctions === "")
      assert(metaData.getSearchStringEscape === "\\")
      assert(metaData.getExtraNameCharacters === "")
      assert(metaData.supportsAlterTableWithAddColumn())
      assert(!metaData.supportsAlterTableWithDropColumn())
      assert(metaData.supportsColumnAliasing())
      assert(metaData.supportsGroupBy)
      assert(!metaData.supportsMultipleResultSets)
      assert(!metaData.supportsNonNullableColumns)
      assert(metaData.supportsOuterJoins)
      assert(metaData.supportsFullOuterJoins)
      assert(metaData.supportsLimitedOuterJoins)
      assert(metaData.getSchemaTerm === "database")
      assert(metaData.getProcedureTerm === "UDF")
      assert(metaData.getCatalogTerm === "instance")
      assert(metaData.getCatalogSeparator === ".")
      assert(metaData.supportsSchemasInDataManipulation)
      assert(!metaData.supportsSchemasInProcedureCalls)
      assert(metaData.supportsSchemasInTableDefinitions)
      assert(!metaData.supportsSchemasInIndexDefinitions)
      assert(!metaData.supportsSchemasInPrivilegeDefinitions)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsCatalogsInDataManipulation)
      assert(!metaData.supportsCatalogsInProcedureCalls)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsCatalogsInTableDefinitions)
      assert(!metaData.supportsCatalogsInIndexDefinitions)
      assert(!metaData.supportsCatalogsInPrivilegeDefinitions)
      assert(!metaData.supportsPositionedDelete)
      assert(!metaData.supportsPositionedUpdate)
      assert(!metaData.supportsSelectForUpdate)
      assert(!metaData.supportsStoredProcedures)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsUnion)
      assert(metaData.supportsUnionAll)
      assert(metaData.getMaxColumnNameLength === 128)
      assert(metaData.getDefaultTransactionIsolation === java.sql.Connection.TRANSACTION_NONE)
      assert(!metaData.supportsTransactions)
      assert(!metaData.getProcedureColumns("", "%", "%", "%").next())
      assert(!metaData.getImportedKeys("", "default", "").next())

      // TODO: SPARK-33219 Disable GetPrimaryKeys and GetCrossReference APIs explicitly
      // for Spark ThriftServer
      assert(!metaData.getPrimaryKeys("", "default", "").next())
      assert(!metaData.getCrossReference("", "default", "src", "", "default", "src2").next())

      assert(!metaData.getIndexInfo("", "default", "src", true, true).next())
      assert(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY))
      assert(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE))
      assert(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE))
      assert(!metaData.supportsBatchUpdates)
      assert(!metaData.getUDTs(",", "%", "%", null).next())
      assert(!metaData.supportsSavepoints)
      assert(!metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT))
      assert(metaData.getJDBCMajorVersion === 3)
      assert(metaData.getJDBCMinorVersion === 0)
      assert(metaData.getSQLStateType === DatabaseMetaData.sqlStateSQL)
      assert(metaData.getMaxLogicalLobSize === 0)
      assert(!metaData.supportsRefCursors)
    }
  }

  test("SPARK-36179: get column operation support TIMESTAMP_[N|L]TZ") {
    val t = "t_ltz_ntz"
    // TODO(SPARK-36180): add hive table here too
    val ddl = s"CREATE GLOBAL TEMP VIEW $t as SELECT TIMESTAMP_LTZ '2018-11-17 13:33:33.000'" +
      " as c0, TIMESTAMP_NTZ '2018-11-17 13:33:33.000' as c1"
    withJdbcStatement(t) { statement =>
      statement.execute(ddl)
      val md = statement.getConnection.getMetaData
      val rowSet = md.getColumns(null, "global_temp", t, "%")

      var idx = 0
      while (rowSet.next()) {
        assert(rowSet.getString("COLUMN_NAME") === "c" + idx)
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.TIMESTAMP)
        assert(rowSet.getString("TYPE_NAME") === "TIMESTAMP" + ("_NTZ" * idx))
        assert(rowSet.getInt("COLUMN_SIZE") === 8)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 6)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 0)
        assert(rowSet.getInt("ORDINAL_POSITION") === idx)
        idx += 1
      }
    }
  }
}
