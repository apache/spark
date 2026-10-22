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
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.execution.python.{UserDefinedPythonFunction, UserDefinedPythonTableFunction}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, FileType, IntegerType, MapType, StructType}

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
    Seq(
      ("PARTITIONED BY (f)", "PARTITION_COLUMN"),
      ("PARTITIONED BY (bucket(4, f))", "PARTITION_COLUMN"),
      ("CLUSTER BY (f)", "CLUSTERING_COLUMN")).foreach { case (clause, usage) =>
      Seq("testcat.t", "t").foreach { table =>
        // Bucketing is only a V2 transform.
        if (table.startsWith("testcat") || !clause.contains("bucket")) {
          val provider = if (table.startsWith("testcat")) "" else "USING parquet"
          withTable(table) {
            checkError(
              exception = intercept[AnalysisException] {
                sql(s"CREATE TABLE $table (id INT, f FILE EXTERNAL) $provider $clause")
              },
              condition = s"FILE_TYPE_UNSUPPORTED_COLUMN_USAGE.$usage",
              parameters = Map("colName" -> "`f`", "dataType" -> "\"FILE EXTERNAL\""))
          }
        }
      }
    }
    Seq("testcat.t", "t").foreach { table =>
      withTable(table) {
        if (table.startsWith("testcat")) {
          sql(s"CREATE TABLE $table (id INT, f FILE EXTERNAL) CLUSTER BY (id)")
        } else {
          // No V1 data source can store a FILE yet, so the table is created in the catalog.
          spark.sessionState.catalog.createTable(CatalogTable(
            identifier = TableIdentifier(table),
            tableType = CatalogTableType.MANAGED,
            storage = CatalogStorageFormat.empty,
            schema = new StructType().add("id", IntegerType).add("f", FileType.external),
            provider = Some("parquet")), ignoreIfExists = false)
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $table CLUSTER BY (f)")
          },
          condition = "FILE_TYPE_UNSUPPORTED_COLUMN_USAGE.CLUSTERING_COLUMN",
          parameters = Map("colName" -> "`f`", "dataType" -> "\"FILE EXTERNAL\""))
      }
    }
  }

  test("a column cannot be altered to a FILE") {
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT, s STRING, f FILE EXTERNAL)")
      Seq("s", "f").foreach { column =>
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE testcat.t ALTER COLUMN $column TYPE FILE")
        }
        assert(e.getCondition === "NOT_SUPPORTED_CHANGE_COLUMN")
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
      s"$externalFile UNION $externalFile",
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
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SELECT /*+ REBALANCE(f) */ * FROM ($externalFile)")
      },
      condition = "UNSUPPORTED_FEATURE.PARTITION_BY_FILE",
      parameters = Map("expr" -> "\"f\"", "dataType" -> "\"FILE EXTERNAL\""),
      context = ExpectedContext(fragment = "/*+ REBALANCE(f) */", start = 7, stop = 25))
  }

  test("a FILE cannot be a key of dropDuplicatesWithinWatermark") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(externalFile).dropDuplicatesWithinWatermark("f")
      },
      condition = "UNSUPPORTED_FEATURE.SET_OPERATION_ON_FILE_TYPE",
      parameters = Map("colName" -> "`f`", "dataType" -> "\"FILE EXTERNAL\""))
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

  test("a FILE cannot be sorted or be a window partition") {
    Seq(
      s"SELECT f FROM ($externalFile) ORDER BY f" -> "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      s"SELECT row_number() OVER (ORDER BY f) FROM ($externalFile)" ->
        "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      s"SELECT count(*) OVER (PARTITION BY f) FROM ($externalFile)" ->
        "EXPRESSION_TYPE_IS_NOT_ORDERABLE").foreach { case (query, condition) =>
      assert(intercept[AnalysisException](sql(query)).getCondition === condition, query)
    }
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
      "(x INT) RETURNS TABLE (f FILE EXTERNAL) RETURN SELECT NULL",
      // Without a RETURNS clause, or with a bare RETURNS TABLE, the return type is inferred.
      "(x INT) RETURN CAST(NULL AS FILE EXTERNAL)",
      "(x INT) RETURNS TABLE RETURN SELECT CAST(NULL AS FILE EXTERNAL) AS f").foreach { signature =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TEMPORARY FUNCTION fn$signature")
        },
        condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
        parameters = Map("routineType" -> "function", "routineName" -> "`fn`"))
    }
    Seq(
      "RETURNS MAP<FILE, INT> RETURN NULL",
      "RETURN CAST(NULL AS MAP<FILE, INT>)").foreach { body =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TEMPORARY FUNCTION fn(x INT) $body")
        },
        condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_MAP_KEY",
        parameters = Map("routineType" -> "function", "routineName" -> "`fn`"))
    }
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
      parameters = Map("routineType" -> "function", "routineName" -> "`file_udf`"))
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("file_udf", fileUdf, MapType(FileType(), IntegerType))
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_MAP_KEY",
      parameters = Map("routineType" -> "function", "routineName" -> "`file_udf`"))
    // A UDF that is not registered is checked when it is used in a query.
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(1).select(udf(fileUdf, FileType.external).withName("inline_udf")(col("id")))
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
      parameters = Map("routineType" -> "function", "routineName" -> "`inline_udf`"))
    spark.udf.register("file_udf", fileUdf, FileType())
  }

  test("a Python UDF and UDTF declare a plain FILE in their return type") {
    // Registration does not run Python, so the function body is never used.
    val func = SimplePythonFunction(
      command = Seq.empty[Byte],
      envVars = new java.util.HashMap[String, String](),
      pythonIncludes = java.util.Collections.emptyList[String](),
      pythonExec = "python3",
      pythonVer = "3",
      broadcastVars = java.util.Collections.emptyList[Broadcast[PythonBroadcast]](),
      accumulator = null)
    def pythonUdf(dataType: DataType): UserDefinedPythonFunction = UserDefinedPythonFunction(
      "py_udf", func, dataType, PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    def pythonUdtf(returnType: Option[StructType]): UserDefinedPythonTableFunction =
      UserDefinedPythonTableFunction(
        "py_udtf", func, returnType, PythonEvalType.SQL_TABLE_UDF, udfDeterministic = true)

    Seq(
      (() => spark.udf.registerPython("py_udf", pythonUdf(FileType.external)), "py_udf"),
      (() => spark.udtf.registerPython("py_udtf",
        pythonUdtf(Some(new StructType().add("f", FileType.external)))), "py_udtf")
    ).foreach { case (register, name) =>
      checkError(
        exception = intercept[AnalysisException](register()),
        condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_QUALIFIER",
        parameters = Map("routineType" -> "function", "routineName" -> s"`$name`"))
    }
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.registerPython("py_udf", pythonUdf(MapType(FileType(), IntegerType)))
      },
      condition = "FILE_TYPE_IN_ROUTINE_SIGNATURE_UNSUPPORTED_MAP_KEY",
      parameters = Map("routineType" -> "function", "routineName" -> "`py_udf`"))

    // A plain FILE is accepted, and a UDTF with an analyze method has no static return type.
    spark.udf.registerPython("py_udf", pythonUdf(FileType()))
    spark.udtf.registerPython("py_udtf", pythonUdtf(Some(new StructType().add("f", FileType()))))
    spark.udtf.registerPython("py_udtf", pythonUdtf(None))
  }
}
