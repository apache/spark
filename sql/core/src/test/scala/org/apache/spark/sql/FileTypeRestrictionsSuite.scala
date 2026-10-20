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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{FileType, IntegerType, MapType}

/**
 * Tests the rules on where the FILE type can be used: table columns must be FILE EXTERNAL,
 * routines declare a plain FILE, and FILE has no defined ordering.
 */
class FileTypeRestrictionsSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)

  // A FILE without a qualifier, at the top level and nested.
  private val unqualifiedFileTypes = Seq(
    "FILE",
    "ARRAY<FILE>",
    "MAP<STRING, FILE>",
    "STRUCT<f1: FILE>",
    "STRUCT<f1: INT, f2: STRUCT<f3: ARRAY<FILE>>>")

  private val externalFile = "SELECT CAST(NULL AS FILE EXTERNAL) AS f"

  test("a table column definition requires FILE EXTERNAL") {
    Seq("testcat.t", "t").foreach { table =>
      val provider = if (table.startsWith("testcat")) "" else "USING parquet"
      unqualifiedFileTypes.foreach { dataType =>
        withTable(table) {
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"CREATE TABLE $table (id INT, c $dataType) $provider")
            },
            condition = "FILE_TYPE_MISSING_QUALIFIER")
        }
      }
    }
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT, c STRUCT<f1: ARRAY<FILE EXTERNAL>>)")
    }
  }

  test("REPLACE TABLE requires FILE EXTERNAL") {
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("REPLACE TABLE testcat.t (id INT, c FILE)")
        },
        condition = "FILE_TYPE_MISSING_QUALIFIER")
    }
  }

  test("ALTER TABLE ADD COLUMNS and REPLACE COLUMNS require FILE EXTERNAL") {
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT)")
      unqualifiedFileTypes.foreach { dataType =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE testcat.t ADD COLUMN c $dataType")
          },
          condition = "FILE_TYPE_MISSING_QUALIFIER")
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE testcat.t REPLACE COLUMNS (id INT, c $dataType)")
          },
          condition = "FILE_TYPE_MISSING_QUALIFIER")
      }
      sql("ALTER TABLE testcat.t ADD COLUMN c FILE EXTERNAL")
    }
  }

  test("CREATE TABLE AS SELECT cannot store a FILE without a qualifier") {
    Seq("testcat.t", "t").foreach { table =>
      val provider = if (table.startsWith("testcat")) "" else "USING parquet"
      withTable(table) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE $table $provider AS SELECT 1 AS id, CAST(NULL AS FILE) AS f")
          },
          condition = "FILE_TYPE_UNKNOWN_WRITE",
          parameters = Map("colNames" -> "[`f`]"))
      }
    }
  }

  test("a FILE column cannot be a partition or clustering column") {
    Seq("PARTITIONED BY (f)", "CLUSTER BY (f)").foreach { clause =>
      withTable("testcat.t") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE testcat.t (id INT, f FILE EXTERNAL) $clause")
          },
          condition = "INVALID_PARTITION_COLUMN_DATA_TYPE",
          parameters = Map("type" -> "\"FILE EXTERNAL\""))
      }
    }
  }

  test("a FILE cannot be a map key") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SELECT map(f, 1) FROM ($externalFile)")
      },
      condition = "DATATYPE_MISMATCH.INVALID_MAP_KEY_TYPE",
      parameters = Map("sqlExpr" -> "\"map(f, 1)\"", "keyType" -> "\"FILE EXTERNAL\""),
      context = ExpectedContext(fragment = "map(f, 1)", start = 7, stop = 15))
    sql(s"SELECT map(1, f) FROM ($externalFile)")
  }

  test("a FILE cannot be a grouping expression") {
    val query = s"SELECT count(*) FROM ($externalFile) GROUP BY f"
    checkError(
      exception = intercept[AnalysisException](sql(query)),
      condition = "GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE",
      parameters = Map("sqlExpr" -> "\"f\"", "dataType" -> "\"FILE EXTERNAL\""),
      context = ExpectedContext(
        fragment = "f", start = query.length - 1, stop = query.length - 1))
  }

  test("a FILE cannot be in INTERSECT, EXCEPT or DISTINCT") {
    Seq(
      s"$externalFile INTERSECT $externalFile",
      s"$externalFile EXCEPT $externalFile",
      s"SELECT DISTINCT f FROM ($externalFile)").foreach { query =>
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        condition = "UNSUPPORTED_FEATURE.SET_OPERATION_ON_FILE_TYPE",
        parameters = Map("colName" -> "`f`", "dataType" -> "\"FILE EXTERNAL\""),
        context = ExpectedContext(fragment = query, start = 0, stop = query.length - 1))
    }
  }

  test("a FILE cannot be a repartition expression") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(externalFile).repartition(5, col("f"))
      },
      condition = "UNSUPPORTED_FEATURE.PARTITION_BY_FILE",
      parameters = Map("expr" -> "\"f\"", "dataType" -> "\"FILE EXTERNAL\""))
  }

  test("a FILE cannot be hashed") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SELECT hash(f) FROM ($externalFile)")
      },
      condition = "DATATYPE_MISMATCH.HASH_FILE_TYPE",
      parameters = Map("sqlExpr" -> "\"hash(f)\"", "functionName" -> "`hash`"),
      context = ExpectedContext(fragment = "hash(f)", start = 7, stop = 13))
  }

  test("a FILE cannot be compared, so it cannot be a join key") {
    val query = s"SELECT 1 FROM ($externalFile) l JOIN ($externalFile) r ON l.f = r.f"
    checkError(
      exception = intercept[AnalysisException](sql(query)),
      condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"(f = f)\"",
        "functionName" -> "`=`",
        "dataType" -> "\"FILE EXTERNAL\""),
      context = ExpectedContext(
        fragment = "l.f = r.f", start = query.length - 9, stop = query.length - 1))
  }

  test("a SQL function declares a plain FILE") {
    Seq(
      "(x FILE EXTERNAL) RETURNS INT RETURN 1",
      "(x INT) RETURNS FILE EXTERNAL RETURN NULL",
      "(x INT) RETURNS ARRAY<FILE EXTERNAL> RETURN NULL",
      "(x INT) RETURNS TABLE (f FILE EXTERNAL) RETURN SELECT NULL").foreach { signature =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TEMPORARY FUNCTION fn$signature")
        },
        condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
        parameters = Map("routineType" -> "function", "routineName" -> "`fn`"))
    }
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY FUNCTION fn(x INT) RETURNS MAP<FILE, INT> RETURN NULL")
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_MAP_KEY",
      parameters = Map("routineType" -> "function", "routineName" -> "`fn`"))
    withUserDefinedFunction("fn" -> true) {
      sql("CREATE TEMPORARY FUNCTION fn(x FILE) RETURNS FILE RETURN x")
    }
  }

  test("a UDF declares a plain FILE in its return type") {
    val fileUdf = new UDF1[Integer, AnyRef] {
      override def call(x: Integer): AnyRef = null
    }
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("file_udf", fileUdf, FileType.external)
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
      parameters = Map("routineType" -> "function", "routineName" -> "file_udf"))
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("file_udf", fileUdf, MapType(FileType(), IntegerType))
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_MAP_KEY",
      parameters = Map("routineType" -> "function", "routineName" -> "file_udf"))
    // A UDF that is not registered is checked when it is used in a query.
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(1).select(udf(fileUdf, FileType.external).withName("inline_udf")(col("id")))
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
      parameters = Map("routineType" -> "function", "routineName" -> "inline_udf"))
    spark.udf.register("file_udf", fileUdf, FileType())
  }
}
