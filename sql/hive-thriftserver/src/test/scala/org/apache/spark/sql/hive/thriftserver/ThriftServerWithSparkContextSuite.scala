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

import java.sql.SQLException

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.HiveSQLException

import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.types._

trait ThriftServerWithSparkContextSuite extends SharedThriftServer {

  test("the scratch dir will be deleted during server start but recreated with new operation") {
    assert(tempScratchDir.exists())
  }

  test("SPARK-29911: Uncache cached tables when session closed") {
    val cacheManager = spark.sharedState.cacheManager
    val globalTempDB = spark.sharedState.globalTempViewManager.database
    withJdbcStatement { statement =>
      statement.execute("CACHE TABLE tempTbl AS SELECT 1")
    }
    // the cached data of local temporary view should be uncached
    assert(cacheManager.isEmpty)
    try {
      withJdbcStatement { statement =>
        statement.execute("CREATE GLOBAL TEMP VIEW globalTempTbl AS SELECT 1, 2")
        statement.execute(s"CACHE TABLE $globalTempDB.globalTempTbl")
      }
      // the cached data of global temporary view shouldn't be uncached
      assert(!cacheManager.isEmpty)
    } finally {
      withJdbcStatement { statement =>
        statement.execute(s"UNCACHE TABLE IF EXISTS $globalTempDB.globalTempTbl")
      }
      assert(cacheManager.isEmpty)
    }
  }

  test("Full stack traces as error message for jdbc or thrift client") {
    val sql = "select date_sub(date'2011-11-11', '1.2')"
    val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]

    withSQLConf((HiveUtils.HIVE_THRIFT_SERVER_ASYNC.key, "false")) {
      withCLIServiceClient { client =>
        val sessionHandle = client.openSession(user, "")
        val e = intercept[HiveSQLException] {
          client.executeStatement(sessionHandle, sql, confOverlay)
        }
        assert(e.getMessage
          .contains("The second argument of 'date_sub' function needs to be an integer."))
        assert(!e.getMessage
          .contains("java.lang.NumberFormatException: invalid input syntax for type numeric: 1.2"))
      }
    }

    withSQLConf((HiveUtils.HIVE_THRIFT_SERVER_ASYNC.key, "true")) {
      withCLIServiceClient { client =>
        val sessionHandle = client.openSession(user, "")
        val opHandle = client.executeStatementAsync(sessionHandle, sql, confOverlay)
        var status = client.getOperationStatus(opHandle)
        while (!status.getState.isTerminal) {
          Thread.sleep(10)
          status = client.getOperationStatus(opHandle)
        }
        val e = status.getOperationException

        assert(e.getMessage
          .contains("The second argument of 'date_sub' function needs to be an integer."))
        assert(e.getMessage
          .contains("java.lang.NumberFormatException: invalid input syntax for type numeric: 1.2"))
      }
    }

    Seq("true", "false").foreach { value =>
      withSQLConf((HiveUtils.HIVE_THRIFT_SERVER_ASYNC.key, value)) {
        withJdbcStatement { statement =>
          val e = intercept[SQLException] {
            statement.executeQuery(sql)
          }
          assert(e.getMessage.contains(
            "The second argument of 'date_sub' function needs to be an integer."))
          assert(e.getMessage.contains(
            "java.lang.NumberFormatException: invalid input syntax for type numeric: 1.2"))
        }
      }
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

    withCLIServiceClient { client =>
      val sessionHandle = client.openSession(user, "")
      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val opHandle = client.executeStatement(sessionHandle, ddl, confOverlay)
      var status = client.getOperationStatus(opHandle)
      while (!status.getState.isTerminal) {
        Thread.sleep(10)
        status = client.getOperationStatus(opHandle)
      }
      val getCol = client.getColumns(sessionHandle, "", schemaName, tableName, null)
      val rowSet = client.fetchResults(getCol)
      val columns = rowSet.toTRowSet.getColumns

      val catalogs = columns.get(0).getStringVal.getValues.asScala
      assert(catalogs.forall(_.isEmpty), "catalog name mismatches")

      val schemas = columns.get(1).getStringVal.getValues.asScala
      assert(schemas.forall(_ == schemaName), "schema name mismatches")

      val tableNames = columns.get(2).getStringVal.getValues.asScala
      assert(tableNames.forall(_ == tableName), "table name mismatches")

      val columnNames = columns.get(3).getStringVal.getValues.asScala
      columnNames.zipWithIndex.foreach {
        case (v, i) => assert(v === "c" + i, "column name mismatches")
      }

      val javaTypes = columns.get(4).getI32Val.getValues
      import java.sql.Types._
      assert(javaTypes.get(0).intValue() === BOOLEAN)
      assert(javaTypes.get(1).intValue() === TINYINT)
      assert(javaTypes.get(2).intValue() === SMALLINT)
      assert(javaTypes.get(3).intValue() === INTEGER)
      assert(javaTypes.get(4).intValue() === BIGINT)
      assert(javaTypes.get(5).intValue() === FLOAT)
      assert(javaTypes.get(6).intValue() === DOUBLE)
      assert(javaTypes.get(7).intValue() === DECIMAL)
      assert(javaTypes.get(8).intValue() === DECIMAL)
      assert(javaTypes.get(9).intValue() === VARCHAR)
      assert(javaTypes.get(10).intValue() === ARRAY)
      assert(javaTypes.get(11).intValue() === ARRAY)
      assert(javaTypes.get(12).intValue() === JAVA_OBJECT)
      assert(javaTypes.get(13).intValue() === DATE)
      assert(javaTypes.get(14).intValue() === TIMESTAMP)
      assert(javaTypes.get(15).intValue() === STRUCT)
      assert(javaTypes.get(16).intValue() === BINARY)

      val typeNames = columns.get(5).getStringVal.getValues.asScala
      typeNames.zip(schema).foreach { case (tName, f) =>
        assert(tName === f.dataType.sql)
      }

      val colSize = columns.get(6).getI32Val.getValues.asScala

      colSize.zip(schema).foreach { case (size, f) =>
        f.dataType match {
          case StringType | BinaryType | _: ArrayType | _: MapType => assert(size === 0)
          case o => assert(size === o.defaultSize)
        }
      }

      val decimalDigits = columns.get(8).getI32Val.getValues.asScala
      decimalDigits.zip(schema).foreach { case (dd, f) =>
        f.dataType match {
          case BooleanType | _: IntegerType => assert(dd === 0)
          case d: DecimalType => assert(dd === d.scale)
          case FloatType => assert(dd === 7)
          case DoubleType => assert(dd === 15)
          case TimestampType => assert(dd === 6)
          case _ => assert(dd === 0) // nulls
        }
      }

      val radixes = columns.get(9).getI32Val.getValues.asScala
      radixes.zip(schema).foreach { case (radix, f) =>
        f.dataType match {
          case _: NumericType => assert(radix === 10)
          case _ => assert(radix === 0) // nulls
        }
      }

      val nullables = columns.get(10).getI32Val.getValues.asScala
      assert(nullables.forall(_ === 1))

      val comments = columns.get(11).getStringVal.getValues.asScala
      comments.zip(schema).foreach { case (c, f) => assert(c === f.getComment().get) }

      val positions = columns.get(16).getI32Val.getValues.asScala
      positions.zipWithIndex.foreach { case (pos, idx) =>
        assert(pos === idx, "the client columns disorder")
      }

      val isNullables = columns.get(17).getStringVal.getValues.asScala
      assert(isNullables.forall(_ === "YES"))

      val autoIncs = columns.get(22).getStringVal.getValues.asScala
      assert(autoIncs.forall(_ === "NO"))
    }
  }
}


class ThriftServerWithSparkContextInBinarySuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.binary
}

class ThriftServerWithSparkContextInHttpSuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.http
}
