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

import java.sql.{DatabaseMetaData, ResultSet}

import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DecimalType, DoubleType, FloatType, IntegerType, MapType, NumericType, StringType, StructType, TimestampType}

class SparkMetadataOperationSuite extends HiveThriftJdbcTest {

  override def mode: ServerMode.Value = ServerMode.binary

  test("Spark's own GetSchemasOperation(SparkGetSchemasOperation)") {
    def checkResult(rs: ResultSet, dbNames: Seq[String]): Unit = {
      for (i <- dbNames.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_SCHEM") === dbNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withDatabase("db1", "db2") { statement =>
      Seq("CREATE DATABASE db1", "CREATE DATABASE db2").foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getSchemas(null, "%"), Seq("db1", "db2", "default", "global_temp"))
      checkResult(metaData.getSchemas(null, "db1"), Seq("db1"))
      checkResult(metaData.getSchemas(null, "db_not_exist"), Seq.empty)
      checkResult(metaData.getSchemas(null, "db*"), Seq("db1", "db2"))
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
    def checkResult(rs: ResultSet, functionName: Seq[String]): Unit = {
      for (i <- functionName.indices) {
        assert(rs.next())
        assert(rs.getString("FUNCTION_SCHEM") === "default")
        assert(rs.getString("FUNCTION_NAME") === functionName(i))
        assert(rs.getString("REMARKS").startsWith(s"${functionName(i)}("))
        assert(rs.getInt("FUNCTION_TYPE") === DatabaseMetaData.functionResultUnknown)
        assert(rs.getString("SPECIFIC_NAME").startsWith("org.apache.spark.sql.catalyst"))
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
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val rs = metaData.getFunctions(null, "default", "upPer")
      assert(rs.next())
      assert(rs.getString("FUNCTION_SCHEM") === "default")
      assert(rs.getString("FUNCTION_NAME") === "upper")
      assert(rs.getString("REMARKS") ===
        "upper(str) - Returns `str` with all characters changed to uppercase.")
      assert(rs.getInt("FUNCTION_TYPE") === DatabaseMetaData.functionResultUnknown)
      assert(rs.getString("SPECIFIC_NAME") === "org.apache.spark.sql.catalyst.expressions.Upper")
      // Make sure there are no more elements
      assert(!rs.next())
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
      checkResult(metaData.getTypeInfo, ThriftserverShimUtils.supportedType().map(_.getName))
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
        DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY)

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
          case StringType | BinaryType | _: ArrayType | _: MapType => assert(colSize === 0)
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

      assert(pos === 17, "all columns should have been verified")
    }
  }
}
