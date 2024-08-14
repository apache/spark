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

import scala.jdk.CollectionConverters.MapHasAsJava

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.{DatasourceV2SQLBase, FakeV2ProviderWithCustomSchema}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.errors.DataTypeErrors.toSQLType
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.{SqlApiConf, SQLConf}
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}

class CollationSuite extends DatasourceV2SQLBase with AdaptiveSparkPlanHelper {
  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName

  private val collationPreservingSources = Seq("parquet")
  private val collationNonPreservingSources = Seq("orc", "csv", "json", "text")
  private val allFileBasedDataSources = collationPreservingSources ++  collationNonPreservingSources

  test("collate returns proper type") {
    Seq("utf8_binary", "utf8_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate $collationName"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate $collationName").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation name is case insensitive") {
    Seq("uTf8_BiNaRy", "utf8_lcase", "uNicOde", "UNICODE_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate $collationName"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate $collationName").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation expression returns name of collation") {
    Seq("utf8_binary", "utf8_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(
        sql(s"select collation('aaa' collate $collationName)"), Row(collationName.toUpperCase()))
    }
  }

  test("collate function syntax") {
    assert(sql(s"select collate('aaa', 'utf8_binary')").schema(0).dataType ==
      StringType("UTF8_BINARY"))
    assert(sql(s"select collate('aaa', 'utf8_lcase')").schema(0).dataType ==
      StringType("UTF8_LCASE"))
  }

  test("collate function syntax with default collation set") {
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_LCASE") {
      assert(sql(s"select collate('aaa', 'utf8_lcase')").schema(0).dataType ==
        StringType("UTF8_LCASE"))
      assert(sql(s"select collate('aaa', 'UNICODE')").schema(0).dataType == StringType("UNICODE"))
    }
  }

  test("collate function syntax invalid arg count") {
    Seq("'aaa','a','b'", "'aaa'", "", "'aaa'").foreach(args => {
      val paramCount = if (args == "") 0 else args.split(',').length.toString
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select collate($args)")
        },
        errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        sqlState = "42605",
        parameters = Map(
          "functionName" -> "`collate`",
          "expectedNum" -> "2",
          "actualNum" -> paramCount.toString,
          "docroot" -> "https://spark.apache.org/docs/latest"),
        context = ExpectedContext(fragment = s"collate($args)", start = 7, stop = 15 + args.length)
      )
    })
  }

  test("collate function invalid collation data type") {
    checkError(
      exception = intercept[AnalysisException](sql("select collate('abc', 123)")),
      errorClass = "UNEXPECTED_INPUT_TYPE",
      sqlState = "42K09",
      Map(
        "functionName" -> "`collate`",
        "paramIndex" -> "first",
        "inputSql" -> "\"123\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRING\""),
      context = ExpectedContext(fragment = s"collate('abc', 123)", start = 7, stop = 25)
    )
  }

  test("NULL as collation name") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("select collate('abc', cast(null as string))") },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      sqlState = "42K09",
      Map("exprName" -> "`collation`", "sqlExpr" -> "\"CAST(NULL AS STRING)\""),
      context = ExpectedContext(
        fragment = s"collate('abc', cast(null as string))", start = 7, stop = 42)
    )
  }

  test("collate function invalid input data type") {
    checkError(
      exception = intercept[ExtendedAnalysisException] { sql(s"select collate(1, 'UTF8_BINARY')") },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      sqlState = "42K09",
      parameters = Map(
        "sqlExpr" -> "\"collate(1, UTF8_BINARY)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"1\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRING\""),
      context = ExpectedContext(
        fragment = s"collate(1, 'UTF8_BINARY')", start = 7, stop = 31))
  }

  test("collation expression returns default collation") {
    checkAnswer(sql(s"select collation('aaa')"), Row("UTF8_BINARY"))
  }

  test("invalid collation name throws exception") {
    checkError(
      exception = intercept[SparkException] { sql("select 'aaa' collate UTF8_BS") },
      errorClass = "COLLATION_INVALID_NAME",
      sqlState = "42704",
      parameters = Map("collationName" -> "UTF8_BS", "proposals" -> "UTF8_LCASE"))
  }

  test("disable bucketing on collated string column") {
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    def createTable(bucketColumns: String*): Unit = {
      val tableName = "test_partition_tbl"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $tableName
             |(id INT, c1 STRING COLLATE UNICODE, c2 string)
             |USING parquet
             |CLUSTERED BY (${bucketColumns.mkString(",")})
             |INTO 4 BUCKETS""".stripMargin
        )
      }
    }
    // should work fine on default collated columns
    createTable("id")
    createTable("c2")
    createTable("id", "c2")

    Seq(Seq("c1"), Seq("c1", "id"), Seq("c1", "c2")).foreach { bucketColumns =>
      checkError(
        exception = intercept[AnalysisException] {
          createTable(bucketColumns: _*)
        },
        errorClass = "INVALID_BUCKET_COLUMN_DATA_TYPE",
        parameters = Map("type" -> "\"STRING COLLATE UNICODE\"")
      );
    }
  }

  test("equality check respects collation") {
    Seq(
      ("utf8_binary", "aaa", "AAA", false),
      ("utf8_binary", "aaa", "aaa", true),
      ("utf8_lcase", "aaa", "aaa", true),
      ("utf8_lcase", "aaa", "AAA", true),
      ("utf8_lcase", "aaa", "bbb", false),
      ("unicode", "aaa", "aaa", true),
      ("unicode", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "aaa", true),
      ("unicode_CI", "aaa", "AAA", true),
      ("unicode_CI", "aaa", "bbb", false)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate $collationName = '$right' collate $collationName"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') = collate('$right', '$collationName')"),
          Row(expected))
    }
  }

  test("comparisons respect collation") {
    Seq(
      ("utf8_binary", "AAA", "aaa", true),
      ("utf8_binary", "aaa", "aaa", false),
      ("utf8_binary", "aaa", "BBB", false),
      ("utf8_lcase", "aaa", "aaa", false),
      ("utf8_lcase", "AAA", "aaa", false),
      ("utf8_lcase", "aaa", "bbb", true),
      ("unicode", "aaa", "aaa", false),
      ("unicode", "aaa", "AAA", true),
      ("unicode", "aaa", "BBB", true),
      ("unicode_CI", "aaa", "aaa", false),
      ("unicode_CI", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "bbb", true)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate $collationName < '$right' collate $collationName"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') < collate('$right', '$collationName')"),
          Row(expected))
    }
  }

  test("checkCollation throws exception for incompatible collationIds") {
    val left: String = "abc" // collate with 'UNICODE_CI'
    val leftCollationName: String = "UNICODE_CI";
    var right: String = null // collate with 'UNICODE'
    val rightCollationName: String = "UNICODE";
    // contains
    right = left.substring(1, 2);
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(s"SELECT contains(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))")
      },
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      sqlState = "42P21",
      parameters = Map(
        "explicitTypes" ->
          s"`string collate $leftCollationName`, `string collate $rightCollationName`"
      )
    )
    // startsWith
    right = left.substring(0, 1);
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(s"SELECT startsWith(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))")
      },
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      sqlState = "42P21",
      parameters = Map(
        "explicitTypes" ->
          s"`string collate $leftCollationName`, `string collate $rightCollationName`"
      )
    )
    // endsWith
    right = left.substring(2, 3);
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(s"SELECT endsWith(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))")
      },
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      sqlState = "42P21",
      parameters = Map(
        "explicitTypes" ->
          s"`string collate $leftCollationName`, `string collate $rightCollationName`"
      )
    )
  }

  test("aggregates count respects collation") {
    Seq(
      ("utf8_binary", Seq("AAA", "aaa"), Seq(Row(1, "AAA"), Row(1, "aaa"))),
      ("utf8_binary", Seq("aaa", "aaa"), Seq(Row(2, "aaa"))),
      ("utf8_binary", Seq("aaa", "bbb"), Seq(Row(1, "aaa"), Row(1, "bbb"))),
      ("utf8_lcase", Seq("aaa", "aaa"), Seq(Row(2, "aaa"))),
      ("utf8_lcase", Seq("AAA", "aaa"), Seq(Row(2, "AAA"))),
      ("utf8_lcase", Seq("aaa", "bbb"), Seq(Row(1, "aaa"), Row(1, "bbb"))),
      ("unicode", Seq("AAA", "aaa"), Seq(Row(1, "AAA"), Row(1, "aaa"))),
      ("unicode", Seq("aaa", "aaa"), Seq(Row(2, "aaa"))),
      ("unicode", Seq("aaa", "bbb"), Seq(Row(1, "aaa"), Row(1, "bbb"))),
      ("unicode_CI", Seq("aaa", "aaa"), Seq(Row(2, "aaa"))),
      ("unicode_CI", Seq("AAA", "aaa"), Seq(Row(2, "AAA"))),
      ("unicode_CI", Seq("aaa", "bbb"), Seq(Row(1, "aaa"), Row(1, "bbb")))
    ).foreach {
      case (collationName: String, input: Seq[String], expected: Seq[Row]) =>
        checkAnswer(sql(
          s"""
          with t as (
          select collate(col1, '$collationName') as c
          from
          values ${input.map(s => s"('$s')").mkString(", ")}
        )
        SELECT COUNT(*), c FROM t GROUP BY c
        """), expected)
    }
  }

  test("hash agg is not used for non binary collations") {
    val tableNameNonBinary = "T_NON_BINARY"
    val tableNameBinary = "T_BINARY"
    withTable(tableNameNonBinary) {
      withTable(tableNameBinary) {
        sql(s"CREATE TABLE $tableNameNonBinary (c STRING COLLATE UTF8_LCASE) USING PARQUET")
        sql(s"INSERT INTO $tableNameNonBinary VALUES ('aaa')")
        sql(s"CREATE TABLE $tableNameBinary (c STRING COLLATE UTF8_BINARY) USING PARQUET")
        sql(s"INSERT INTO $tableNameBinary VALUES ('aaa')")

        val dfNonBinary = sql(s"SELECT COUNT(*), c FROM $tableNameNonBinary GROUP BY c")
        assert(collectFirst(dfNonBinary.queryExecution.executedPlan) {
          case _: HashAggregateExec | _: ObjectHashAggregateExec => ()
        }.isEmpty)

        val dfBinary = sql(s"SELECT COUNT(*), c FROM $tableNameBinary GROUP BY c")
        assert(collectFirst(dfBinary.queryExecution.executedPlan) {
          case _: HashAggregateExec | _: ObjectHashAggregateExec => ()
        }.nonEmpty)
      }
    }
  }

  test("text writing to parquet with collation enclosed with backticks") {
    withTempPath{ path =>
      sql(s"select 'a' COLLATE `UNICODE`").write.parquet(path.getAbsolutePath)

      checkAnswer(
        spark.read.parquet(path.getAbsolutePath),
        Row("a"))
    }
  }

  test("create table with collation") {
    val tableName = "dummy_tbl"
    val collationName = "UTF8_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    allFileBasedDataSources.foreach { format =>
      withTable(tableName) {
        sql(
        s"""
           |CREATE TABLE $tableName (
           |  c1 STRING COLLATE $collationName
           |)
           |USING $format
           |""".stripMargin)

        sql(s"INSERT INTO $tableName VALUES ('aaa')")
        sql(s"INSERT INTO $tableName VALUES ('AAA')")

        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"), Seq(Row(collationName)))
        assert(sql(s"select c1 FROM $tableName").schema.head.dataType == StringType(collationId))
      }
    }
  }

  test("write collated data to different data sources with dataframe api") {
    val collationName = "UNICODE_CI"

    allFileBasedDataSources.foreach { format =>
      withTempPath { path =>
        val df = sql(s"SELECT c COLLATE $collationName AS c FROM VALUES ('aaa') AS data(c)")
        df.write.format(format).save(path.getAbsolutePath)

        val readback = spark.read.format(format).load(path.getAbsolutePath)
        val readbackCollation = if (collationPreservingSources.contains(format)) {
          collationName
        } else {
          "UTF8_BINARY"
        }

        checkAnswer(readback, Row("aaa"))
        checkAnswer(
          readback.selectExpr(s"collation(${readback.columns.head})"),
          Row(readbackCollation))
      }
    }
  }

  test("add collated column with alter table") {
    val tableName = "alter_column_tbl"
    val defaultCollation = "UTF8_BINARY"
    val collationName = "UTF8_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (c1 STRING)
           |USING PARQUET
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"),
        Seq(Row(defaultCollation)))

      sql(
        s"""
           |ALTER TABLE $tableName
           |ADD COLUMN c2 STRING COLLATE $collationName
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('aaa', 'aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA', 'AAA')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $tableName"),
        Seq(Row(collationName)))
      assert(sql(s"select c2 FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }

  test("SPARK-47210: Implicit casting of collated strings") {
    val tableName = "parquet_dummy_implicit_cast_t22"
    withTable(tableName) {
      spark.sql(
        s"""
           | CREATE TABLE $tableName(c1 STRING COLLATE UTF8_LCASE,
           | c2 STRING COLLATE UNICODE, c3 STRING COLLATE UNICODE_CI, c4 STRING)
           | USING PARQUET
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES ('a', 'a', 'a', 'a')")
      sql(s"INSERT INTO $tableName VALUES ('A', 'A', 'A', 'A')")

      // collate literal to c1's collation
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 = 'a'"),
        Seq(Row("a"), Row("A")))
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE 'a' = c1"),
        Seq(Row("a"), Row("A")))

      // collate c1 to UTF8_BINARY because it is explicitly set
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 = COLLATE('a', 'UTF8_BINARY')"),
        Seq(Row("a")))

      // fail with implicit mismatch, as function return should be considered implicit
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 FROM $tableName " +
            s"WHERE c1 = SUBSTR(COLLATE('a', 'UNICODE'), 0)")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT",
        parameters = Map.empty
      )

      // in operator
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 IN ('a')"),
        Seq(Row("a"), Row("A")))
      // explicitly set collation inside IN operator
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 IN ('b', COLLATE('a', 'UTF8_BINARY'))"),
        Seq(Row("a")))

      // concat without type mismatch
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 || 'a' || 'a' = 'aaa'"),
        Seq(Row("a"), Row("A")))
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 || COLLATE(c2, 'UTF8_BINARY') = 'aa'"),
        Seq(Row("a")))

      // concat of columns of different collations is allowed
      // as long as we don't use the result in an unsupported function
      // TODO(SPARK-47210): Add indeterminate support
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 || c2 FROM $tableName")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )


      // concat + in
      checkAnswer(sql(s"SELECT c1 FROM $tableName where c1 || 'a' " +
        s"IN (COLLATE('aa', 'UTF8_LCASE'))"), Seq(Row("a"), Row("A")))
      checkAnswer(sql(s"SELECT c1 FROM $tableName where (c1 || 'a') " +
        s"IN (COLLATE('aa', 'UTF8_BINARY'))"), Seq(Row("a")))

      // columns have different collation
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 FROM $tableName WHERE c1 = c3")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )

      // different explicit collations are set
      checkError(
        exception = intercept[AnalysisException] {
          sql(
            s"""
               |SELECT c1 FROM $tableName
               |WHERE COLLATE('a', 'UTF8_BINARY') = COLLATE('a', 'UNICODE')"""
              .stripMargin)
        },
        errorClass = "COLLATION_MISMATCH.EXPLICIT",
        parameters = Map(
          "explicitTypes" -> "`string`, `string collate UNICODE`"
        )
      )

      // in operator has different collations
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 FROM $tableName WHERE c1 IN " +
            "(COLLATE('a', 'UTF8_BINARY'), COLLATE('b', 'UNICODE'))")
        },
        errorClass = "COLLATION_MISMATCH.EXPLICIT",
        parameters = Map(
          "explicitTypes" -> "`string`, `string collate UNICODE`"
        )
      )
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 FROM $tableName WHERE COLLATE(c1, 'UNICODE') IN " +
            "(COLLATE('a', 'UTF8_BINARY'))")
        },
        errorClass = "COLLATION_MISMATCH.EXPLICIT",
        parameters = Map(
          "explicitTypes" -> "`string collate UNICODE`, `string`"
        )
      )

      // concat on different implicit collations should succeed,
      // but should fail on try of comparison
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT c1 FROM $tableName WHERE c1 || c3 = 'aa'")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )

      // concat on different implicit collations should succeed,
      // but should fail on try of ordering
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM $tableName ORDER BY c1 || c3")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )

      // concat + in
      checkAnswer(sql(s"SELECT c1 FROM $tableName WHERE c1 || COLLATE('a', 'UTF8_BINARY') IN " +
        s"(COLLATE('aa', 'UNICODE'))"),
        Seq(Row("a")))

      // array creation supports implicit casting
      checkAnswer(sql(s"SELECT typeof(array('a' COLLATE UNICODE, 'b')[1])"),
        Seq(Row("string collate UNICODE")))

      // contains fails with indeterminate collation
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM $tableName WHERE contains(c1||c3, 'a')")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT array('A', 'a' COLLATE UNICODE) == array('b' COLLATE UNICODE_CI)")
        },
        errorClass = "COLLATION_MISMATCH.IMPLICIT"
      )

      checkAnswer(sql("SELECT array_join(array('a', 'b' collate UNICODE), 'c' collate UNICODE_CI)"),
        Seq(Row("acb")))
    }
  }

  test("SPARK-49138: ArrayAppend and CreateMap coercion testing") {
    val df_array_append = sql(s"SELECT array_append(array('a', 'b'), 'c' COLLATE UNICODE)")
    // array_append expression
    checkAnswer(df_array_append, Seq(Row(Seq("a", "b", "c"))))
    assert(df_array_append.schema.head.dataType == ArrayType(StringType("UNICODE"), true))

    // make sure we fail this query even when collations are in
    checkError(
      exception = intercept[AnalysisException] {
        sql("select map('a' COLLATE UTF8_LCASE, 'b', 'c')")
      },
      errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map("functionName" -> "`map`", "expectedNum" -> "2n (n > 0)",
        "actualNum" -> "3", "docroot" -> "https://spark.apache.org/docs/latest")
    )

    // make sure we fail this query even when collations are in
    checkError(
      exception = intercept[AnalysisException] {
        sql("select map('a' COLLATE UTF8_LCASE, 'b', 'c' COLLATE UNICODE, 'c')")
      },
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      sqlState = "42P21",
      parameters = Map(
        "explicitTypes" ->
          s"`string collate UTF8_LCASE`, `string collate UNICODE`"
      )
    )

    // map creation keys respects proper collation
    val df_create_map = sql("select map('a' COLLATE UTF8_LCASE, 'b', 'c', 'c')")
    checkAnswer(df_create_map, Seq(Row(Map("a" -> "b", "c" -> "c"))))
    assert(df_create_map.schema.head.dataType ==
      MapType(StringType("UTF8_LCASE"), StringType("UTF8_BINARY"), false))
  }

  test("SPARK-47692: Parameter marker with EXECUTE IMMEDIATE implicit casting") {
    sql(s"DECLARE stmtStr1 = 'SELECT collation(:var1 || :var2)';")
    sql(s"DECLARE stmtStr2 = 'SELECT collation(:var1 || (\\\'a\\\' COLLATE UNICODE))';")

    checkAnswer(
      sql(
        """EXECUTE IMMEDIATE stmtStr1 USING
          | 'a' AS var1,
          | 'b' AS var2;""".stripMargin),
      Seq(Row("UTF8_BINARY"))
    )

    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      checkAnswer(
        sql(
          """EXECUTE IMMEDIATE stmtStr1 USING
            | 'a' AS var1,
            | 'b' AS var2;""".stripMargin),
        Seq(Row("UNICODE"))
      )
    }

    checkAnswer(
      sql(
        """EXECUTE IMMEDIATE stmtStr2 USING
          | 'a' AS var1;""".stripMargin),
      Seq(Row("UNICODE"))
    )

    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      checkAnswer(
        sql(
          """EXECUTE IMMEDIATE stmtStr2 USING
            | 'a' AS var1;""".stripMargin),
        Seq(Row("UNICODE"))
      )
    }
  }

  test("SPARK-47692: Parameter markers with variable mapping") {
    checkAnswer(
      spark.sql(
        "SELECT collation(:var1 || :var2)",
        Map("var1" -> Literal.create('a', StringType("UTF8_BINARY")),
            "var2" -> Literal.create('b', StringType("UNICODE")))),
      Seq(Row("UTF8_BINARY"))
    )

    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      checkAnswer(
        spark.sql(
          "SELECT collation(:var1 || :var2)",
          Map("var1" -> Literal.create('a', StringType("UTF8_BINARY")),
              "var2" -> Literal.create('b', StringType("UNICODE")))),
        Seq(Row("UNICODE"))
      )
    }
  }

  test("SPARK-47210: Cast of default collated strings in IN expression") {
    val tableName = "t1"
    withTable(tableName) {
      spark.sql(
        s"""
           | CREATE TABLE $tableName(utf8_binary STRING COLLATE UTF8_BINARY,
           | utf8_lcase STRING COLLATE UTF8_LCASE)
           | USING PARQUET
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES ('aaa', 'aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA', 'AAA')")
      sql(s"INSERT INTO $tableName VALUES ('bbb', 'bbb')")
      sql(s"INSERT INTO $tableName VALUES ('BBB', 'BBB')")

      checkAnswer(sql(s"SELECT * FROM $tableName " +
        s"WHERE utf8_lcase IN " +
        s"('aaa' COLLATE UTF8_LCASE, 'bbb' COLLATE UTF8_LCASE)"),
        Seq(Row("aaa", "aaa"), Row("AAA", "AAA"), Row("bbb", "bbb"), Row("BBB", "BBB")))
      checkAnswer(sql(s"SELECT * FROM $tableName " +
        s"WHERE utf8_lcase IN ('aaa' COLLATE UTF8_LCASE, 'bbb')"),
        Seq(Row("aaa", "aaa"), Row("AAA", "AAA"), Row("bbb", "bbb"), Row("BBB", "BBB")))
    }
  }

  // TODO(SPARK-47210): Add indeterminate support
  test("SPARK-47210: Indeterminate collation checks") {
    val tableName = "t1"
    val newTableName = "t2"
    withTable(tableName) {
      spark.sql(
        s"""
           | CREATE TABLE $tableName(c1 STRING COLLATE UNICODE,
           | c2 STRING COLLATE UTF8_LCASE)
           | USING PARQUET
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES ('aaa', 'aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA', 'AAA')")
      sql(s"INSERT INTO $tableName VALUES ('bbb', 'bbb')")
      sql(s"INSERT INTO $tableName VALUES ('BBB', 'BBB')")

      withSQLConf("spark.sql.legacy.createHiveTableByDefault" -> "false") {
        withTable(newTableName) {
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"CREATE TABLE $newTableName AS SELECT c1 || c2 FROM $tableName")
            },
            errorClass = "COLLATION_MISMATCH.IMPLICIT")
        }
      }
    }
  }

  test("create v2 table with collation column") {
    val tableName = "testcat.table_name"
    val collationName = "UTF8_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (c1 string COLLATE $collationName)
           |USING $v2Source
           |""".stripMargin)

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

      assert(table.name == tableName)
      assert(table.partitioning.isEmpty)
      assert(table.properties == withDefaultOwnership(Map("provider" -> v2Source)).asJava)
      assert(table.columns().head.dataType() == StringType(collationId))

      val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
      checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)

      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"),
        Seq(Row(collationName)))
      assert(sql(s"select c1 FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }

  test("disable partition on collated string column") {
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    def createTable(partitionColumns: String*): Unit = {
      val tableName = "test_partition_tbl"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $tableName
             |(id INT, c1 STRING COLLATE UNICODE, c2 string)
             |USING parquet
             |PARTITIONED BY (${partitionColumns.mkString(",")})
             |""".stripMargin)
      }
    }

    // should work fine on non collated columns
    createTable("id")
    createTable("c2")
    createTable("id", "c2")

    Seq(Seq("c1"), Seq("c1", "id"), Seq("c1", "c2")).foreach { partitionColumns =>
      checkError(
        exception = intercept[AnalysisException] {
          createTable(partitionColumns: _*)
        },
        errorClass = "INVALID_PARTITION_COLUMN_DATA_TYPE",
        parameters = Map("type" -> "\"STRING COLLATE UNICODE\"")
      );
    }
  }

  test("shuffle respects collation") {
    val in = (('a' to 'z') ++ ('A' to 'Z')).map(_.toString * 3).map(Row.apply(_))

    val schema = StructType(StructField(
      "col",
      StringType(CollationFactory.collationNameToId("UTF8_LCASE"))) :: Nil)
    val df = spark.createDataFrame(sparkContext.parallelize(in), schema)

    df.repartition(10, df.col("col")).foreachPartition(
      (rowIterator: Iterator[Row]) => {
        val partitionData = rowIterator.map(r => r.getString(0)).toArray
        partitionData.foreach(s => {
          // assert that both lower and upper case of the string are present in the same partition.
          assert(partitionData.contains(s.toLowerCase()))
          assert(partitionData.contains(s.toUpperCase()))
        })
    })
  }

  test("Generated column expressions using collations - errors out") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TABLE testcat.test_table(
             |  c1 STRING COLLATE UNICODE,
             |  c2 STRING COLLATE UNICODE GENERATED ALWAYS AS (SUBSTRING(c1, 0, 1))
             |)
             |USING $v2Source
             |""".stripMargin)
      },
      errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "c2",
        "expressionStr" -> "SUBSTRING(c1, 0, 1)",
        "reason" ->
          "generation expression cannot contain non utf8 binary collated string type"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TABLE testcat.test_table(
             |  c1 STRING COLLATE UNICODE,
             |  c2 STRING COLLATE UNICODE GENERATED ALWAYS AS (LOWER(c1))
             |)
             |USING $v2Source
             |""".stripMargin)
      },
      errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "c2",
        "expressionStr" -> "LOWER(c1)",
        "reason" ->
          "generation expression cannot contain non utf8 binary collated string type"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TABLE testcat.test_table(
             |  struct1 STRUCT<a: STRING COLLATE UNICODE>,
             |  c2 STRING COLLATE UNICODE GENERATED ALWAYS AS (UCASE(struct1.a))
             |)
             |USING $v2Source
             |""".stripMargin)
      },
      errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "c2",
        "expressionStr" -> "UCASE(struct1.a)",
        "reason" ->
          "generation expression cannot contain non utf8 binary collated string type"))
  }

  test("SPARK-47431: Default collation set to UNICODE, literal test") {
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      checkAnswer(sql(s"SELECT collation('aa')"), Seq(Row("UNICODE")))
    }
  }

  test("SPARK-47972: Cast expression limitation for collations") {
    checkError(
      exception = intercept[ParseException]
        (sql("SELECT cast(1 as string collate unicode)")),
      errorClass = "UNSUPPORTED_DATATYPE",
      parameters = Map(
        "typeName" -> toSQLType(StringType("UNICODE"))),
      context =
        ExpectedContext(fragment = s"cast(1 as string collate unicode)", start = 7, stop = 39)
    )

    checkError(
      exception = intercept[ParseException]
        (sql("SELECT 'A' :: string collate unicode")),
      errorClass = "UNSUPPORTED_DATATYPE",
      parameters = Map(
        "typeName" -> toSQLType(StringType("UNICODE"))),
      context = ExpectedContext(fragment = s"'A' :: string collate unicode", start = 7, stop = 35)
    )

    checkAnswer(sql(s"SELECT cast(1 as string)"), Seq(Row("1")))
    checkAnswer(sql(s"SELECT cast('A' as string)"), Seq(Row("A")))

    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      checkError(
        exception = intercept[ParseException]
          (sql("SELECT cast(1 as string collate unicode)")),
        errorClass = "UNSUPPORTED_DATATYPE",
        parameters = Map(
          "typeName" -> toSQLType(StringType("UNICODE"))),
        context =
          ExpectedContext(fragment = s"cast(1 as string collate unicode)", start = 7, stop = 39)
      )

      checkAnswer(sql(s"SELECT cast(1 as string)"), Seq(Row("1")))
      checkAnswer(sql(s"SELECT collation(cast(1 as string))"), Seq(Row("UNICODE")))
    }
  }

  test("SPARK-47431: Default collation set to UNICODE, column type test") {
    withTable("t") {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
        sql(s"CREATE TABLE t(c1 STRING) USING PARQUET")
        sql(s"INSERT INTO t VALUES ('a')")
        checkAnswer(sql(s"SELECT collation(c1) FROM t"), Seq(Row("UNICODE")))
      }
    }
  }

  test("SPARK-47431: Create table with UTF8_BINARY, make sure collation persists on read") {
    withTable("t") {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_BINARY") {
        sql("CREATE TABLE t(c1 STRING) USING PARQUET")
        sql("INSERT INTO t VALUES ('a')")
        checkAnswer(sql("SELECT collation(c1) FROM t"), Seq(Row("UTF8_BINARY")))
      }
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
        checkAnswer(sql("SELECT collation(c1) FROM t"), Seq(Row("UTF8_BINARY")))
      }
    }
  }

  test("Create dataframe with non utf8 binary collation") {
    val schema = StructType(Seq(StructField("Name", StringType("UNICODE_CI"))))
    val data = Seq(Row("Alice"), Row("Bob"), Row("bob"))
    val df = spark.createDataFrame(sparkContext.parallelize(data), schema)

    checkAnswer(
      df.groupBy("name").count(),
      Seq(Row("Alice", 1), Row("Bob", 2))
    )
  }

  test("Aggregation on complex containing collated strings") {
    val table = "table_agg"
    // array
    withTable(table) {
      sql(s"create table $table (a array<string collate utf8_lcase>) using parquet")
      sql(s"insert into $table values (array('aaa')), (array('AAA'))")
      checkAnswer(sql(s"select distinct a from $table"), Seq(Row(Seq("aaa"))))
    }
    // map doesn't support aggregation
    withTable(table) {
      sql(s"create table $table (m map<string collate utf8_lcase, string>) using parquet")
      val query = s"select distinct m from $table"
      checkError(
        exception = intercept[ExtendedAnalysisException](sql(query)),
        errorClass = "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
        parameters = Map(
          "colName" -> "`m`",
          "dataType" -> toSQLType(MapType(
            StringType(CollationFactory.collationNameToId("UTF8_LCASE")),
            StringType))),
        context = ExpectedContext(query, 0, query.length - 1)
      )
    }
    // struct
    withTable(table) {
      sql(s"create table $table (s struct<fld:string collate utf8_lcase>) using parquet")
      sql(s"insert into $table values (named_struct('fld', 'aaa')), (named_struct('fld', 'AAA'))")
      checkAnswer(sql(s"select s.fld from $table group by s"), Seq(Row("aaa")))
    }
  }

  test("Joins on complex types containing collated strings") {
    val tableLeft = "table_join_le"
    val tableRight = "table_join_ri"
    // array
    withTable(tableLeft, tableRight) {
      Seq(tableLeft, tableRight).map(tab =>
        sql(s"create table $tab (a array<string collate utf8_lcase>) using parquet"))
      Seq((tableLeft, "array('aaa')"), (tableRight, "array('AAA')")).map{
        case (tab, data) => sql(s"insert into $tab values ($data)")
      }
      checkAnswer(sql(
        s"""
           |select $tableLeft.a from $tableLeft
           |join $tableRight on $tableLeft.a = $tableRight.a
           |""".stripMargin), Seq(Row(Seq("aaa"))))
    }
    // map doesn't support joins
    withTable(tableLeft, tableRight) {
      Seq(tableLeft, tableRight).map(tab =>
        sql(s"create table $tab (m map<string collate utf8_lcase, string>) using parquet"))
      val query =
        s"select $tableLeft.m from $tableLeft join $tableRight on $tableLeft.m = $tableRight.m"
      val ctx = s"$tableLeft.m = $tableRight.m"
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        errorClass = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
        parameters = Map(
          "functionName" -> "`=`",
          "dataType" -> toSQLType(MapType(
            StringType(CollationFactory.collationNameToId("UTF8_LCASE")),
            StringType
          )),
          "sqlExpr" -> "\"(m = m)\""),
        context = ExpectedContext(ctx, query.length - ctx.length, query.length - 1))
    }
    // struct
    withTable(tableLeft, tableRight) {
      Seq(tableLeft, tableRight).map(tab =>
        sql(s"create table $tab (s struct<fld:string collate utf8_lcase>) using parquet"))
      Seq(
        (tableLeft, "named_struct('fld', 'aaa')"),
        (tableRight, "named_struct('fld', 'AAA')")
      ).map {
        case (tab, data) => sql(s"insert into $tab values ($data)")
      }
      checkAnswer(sql(
        s"""
           |select $tableLeft.s.fld from $tableLeft
           |join $tableRight on $tableLeft.s = $tableRight.s
           |""".stripMargin), Seq(Row("aaa")))
    }
  }

  test("Support operations on complex types containing collated strings") {
    checkAnswer(sql("select reverse('abc' collate utf8_lcase)"), Seq(Row("cba")))
    checkAnswer(sql(
      """
        |select reverse(array('a' collate utf8_lcase,
        |'b' collate utf8_lcase))
        |""".stripMargin), Seq(Row(Seq("b", "a"))))
    checkAnswer(sql(
      """
        |select array_join(array('a' collate utf8_lcase,
        |'b' collate utf8_lcase), ', ' collate utf8_lcase)
        |""".stripMargin), Seq(Row("a, b")))
    checkAnswer(sql(
      """
        |select array_join(array('a' collate utf8_lcase,
        |'b' collate utf8_lcase, null), ', ' collate utf8_lcase,
        |'c' collate utf8_lcase)
        |""".stripMargin), Seq(Row("a, b, c")))
    checkAnswer(sql(
      """
        |select concat('a' collate utf8_lcase, 'b' collate utf8_lcase)
        |""".stripMargin), Seq(Row("ab")))
    checkAnswer(sql(
      """
        |select concat(array('a' collate utf8_lcase, 'b' collate utf8_lcase))
        |""".stripMargin), Seq(Row(Seq("a", "b"))))
    checkAnswer(sql(
      """
        |select map('a' collate utf8_lcase, 1, 'b' collate utf8_lcase, 2)
        |['A' collate utf8_lcase]
        |""".stripMargin), Seq(Row(1)))
    val ctx = "map('aaa' collate utf8_lcase, 1, 'AAA' collate utf8_lcase, 2)['AaA']"
    val query = s"select $ctx"
    checkError(
      exception = intercept[AnalysisException](sql(query)),
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"map(collate(aaa, utf8_lcase), 1, collate(AAA, utf8_lcase), 2)[AaA]\"",
        "paramIndex" -> "second",
        "inputSql" -> "\"AaA\"",
        "inputType" -> toSQLType(StringType),
        "requiredType" -> toSQLType(StringType(
          CollationFactory.collationNameToId("UTF8_LCASE")))
      ),
      context = ExpectedContext(
        fragment = ctx,
        start = query.length - ctx.length,
        stop = query.length - 1
      )
    )
  }

  test("window aggregates should respect collation") {
    val t1 = "T_NON_BINARY"
    val t2 = "T_BINARY"

    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (c STRING COLLATE UTF8_LCASE, i int) USING PARQUET")
      sql(s"INSERT INTO $t1 VALUES ('aA', 2), ('Aa', 1), ('ab', 3), ('aa', 1)")

      sql(s"CREATE TABLE $t2 (c STRING, i int) USING PARQUET")
      // Same input but already normalized to lowercase.
      sql(s"INSERT INTO $t2 VALUES ('aa', 2), ('aa', 1), ('ab', 3), ('aa', 1)")

      val dfNonBinary =
        sql(s"SELECT lower(c), i, nth_value(i, 2) OVER (PARTITION BY c ORDER BY i) FROM $t1")
      val dfBinary =
        sql(s"SELECT c, i, nth_value(i, 2) OVER (PARTITION BY c ORDER BY i) FROM $t2")
      checkAnswer(dfNonBinary, dfBinary)
    }
  }

  test("hash join should be used for collated strings") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashJoinTestCase[R](collation: String, result: R)
    val testCases = Seq(
      HashJoinTestCase("UTF8_BINARY", Seq(Row("aa", 1, "aa", 2))),
      HashJoinTestCase("UTF8_LCASE", Seq(Row("aa", 1, "AA", 2), Row("aa", 1, "aa", 2))),
      HashJoinTestCase("UNICODE", Seq(Row("aa", 1, "aa", 2))),
      HashJoinTestCase("UNICODE_CI", Seq(Row("aa", 1, "AA", 2), Row("aa", 1, "aa", 2)))
    )

    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x STRING COLLATE ${t.collation}, i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES ('aa', 1)")

        sql(s"CREATE TABLE $t2 (y STRING COLLATE ${t.collation}, j int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES ('AA', 2), ('aa', 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // confirm that hash join is used instead of sort merge join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: SortMergeJoinExec => ()
          }.isEmpty
        )

        // Only if collation doesn't support binary equality, collation key should be injected.
        if (!CollationFactory.fetchCollation(t.collation).supportsBinaryEquality) {
          assert(collectFirst(queryPlan) {
            case b: HashJoin => b.leftKeys.head
          }.head.isInstanceOf[CollationKey])
        } else {
          assert(!collectFirst(queryPlan) {
            case b: HashJoin => b.leftKeys.head
          }.head.isInstanceOf[CollationKey])
        }
      }
    })
  }

  test("hash join should be used for arrays of collated strings") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashJoinTestCase[R](collation: String, result: R)
    val testCases = Seq(
      HashJoinTestCase("UTF8_BINARY",
        Seq(Row(Seq("aa"), 1, Seq("aa"), 2))),
      HashJoinTestCase("UTF8_LCASE",
        Seq(Row(Seq("aa"), 1, Seq("AA"), 2), Row(Seq("aa"), 1, Seq("aa"), 2))),
      HashJoinTestCase("UNICODE",
        Seq(Row(Seq("aa"), 1, Seq("aa"), 2))),
      HashJoinTestCase("UNICODE_CI",
        Seq(Row(Seq("aa"), 1, Seq("AA"), 2), Row(Seq("aa"), 1, Seq("aa"), 2)))
    )

    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x ARRAY<STRING COLLATE ${t.collation}>, i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES (array('aa'), 1)")

        sql(s"CREATE TABLE $t2 (y ARRAY<STRING COLLATE ${t.collation}>, j int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES (array('AA'), 2), (array('aa'), 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // confirm that hash join is used instead of sort merge join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: ShuffledJoin => ()
          }.isEmpty
        )

        // Only if collation doesn't support binary equality, collation key should be injected.
        if (!CollationFactory.fetchCollation(t.collation).supportsBinaryEquality) {
          assert(collectFirst(queryPlan) {
            case b: BroadcastHashJoinExec => b.leftKeys.head
          }.head.asInstanceOf[ArrayTransform].function.asInstanceOf[LambdaFunction].
            function.isInstanceOf[CollationKey])
        } else {
          assert(!collectFirst(queryPlan) {
            case b: BroadcastHashJoinExec => b.leftKeys.head
          }.head.isInstanceOf[ArrayTransform])
        }
      }
    })
  }

  test("hash join should be used for arrays of arrays of collated strings") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashJoinTestCase[R](collation: String, result: R)
    val testCases = Seq(
      HashJoinTestCase("UTF8_BINARY",
        Seq(Row(Seq(Seq("aa")), 1, Seq(Seq("aa")), 2))),
      HashJoinTestCase("UTF8_LCASE",
        Seq(Row(Seq(Seq("aa")), 1, Seq(Seq("AA")), 2), Row(Seq(Seq("aa")), 1, Seq(Seq("aa")), 2))),
      HashJoinTestCase("UNICODE",
        Seq(Row(Seq(Seq("aa")), 1, Seq(Seq("aa")), 2))),
      HashJoinTestCase("UNICODE_CI",
        Seq(Row(Seq(Seq("aa")), 1, Seq(Seq("AA")), 2), Row(Seq(Seq("aa")), 1, Seq(Seq("aa")), 2)))
    )

    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x ARRAY<ARRAY<STRING COLLATE ${t.collation}>>, i int) USING " +
          s"PARQUET")
        sql(s"INSERT INTO $t1 VALUES (array(array('aa')), 1)")

        sql(s"CREATE TABLE $t2 (y ARRAY<ARRAY<STRING COLLATE ${t.collation}>>, j int) USING " +
          s"PARQUET")
        sql(s"INSERT INTO $t2 VALUES (array(array('AA')), 2), (array(array('aa')), 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // confirm that hash join is used instead of sort merge join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: ShuffledJoin => ()
          }.isEmpty
        )

        // Only if collation doesn't support binary equality, collation key should be injected.
        if (!CollationFactory.fetchCollation(t.collation).supportsBinaryEquality) {
          assert(collectFirst(queryPlan) {
            case b: BroadcastHashJoinExec => b.leftKeys.head
          }.head.asInstanceOf[ArrayTransform].function.
            asInstanceOf[LambdaFunction].function.asInstanceOf[ArrayTransform].function.
            asInstanceOf[LambdaFunction].function.isInstanceOf[CollationKey])
        } else {
          assert(!collectFirst(queryPlan) {
            case b: BroadcastHashJoinExec => b.leftKeys.head
          }.head.isInstanceOf[ArrayTransform])
        }
      }
    })
  }

  test("hash join should respect collation for struct of strings") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashJoinTestCase[R](collation: String, result: R)
    val testCases = Seq(
      HashJoinTestCase("UTF8_BINARY",
        Seq(Row(Row("aa"), 1, Row("aa"), 2))),
      HashJoinTestCase("UTF8_LCASE",
        Seq(Row(Row("aa"), 1, Row("AA"), 2), Row(Row("aa"), 1, Row("aa"), 2))),
      HashJoinTestCase("UNICODE",
        Seq(Row(Row("aa"), 1, Row("aa"), 2))),
      HashJoinTestCase("UNICODE_CI",
        Seq(Row(Row("aa"), 1, Row("AA"), 2), Row(Row("aa"), 1, Row("aa"), 2)))
    )
    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x STRUCT<f:STRING COLLATE ${t.collation}>, i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES (named_struct('f', 'aa'), 1)")

        sql(s"CREATE TABLE $t2 (y STRUCT<f:STRING COLLATE ${t.collation}>, j int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES (named_struct('f', 'AA'), 2), (named_struct('f', 'aa'), 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // Confirm that hash join is used instead of sort merge join.
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: ShuffledJoin => ()
          }.isEmpty
        )

        // Only if collation doesn't support binary equality, collation key should be injected.
        if (!CollationFactory.fetchCollation(t.collation).supportsBinaryEquality) {
          assert(queryPlan.toString().contains("collationkey"))
        } else {
          assert(!queryPlan.toString().contains("collationkey"))
        }
      }
    })
  }

  test("hash join should respect collation for struct of array of struct of strings") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashJoinTestCase[R](collation: String, result: R)
    val testCases = Seq(
      HashJoinTestCase("UTF8_BINARY",
        Seq(Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("aa"))), 2))),
      HashJoinTestCase("UTF8_LCASE",
        Seq(Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("AA"))), 2),
          Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("aa"))), 2))),
      HashJoinTestCase("UNICODE",
        Seq(Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("aa"))), 2))),
      HashJoinTestCase("UNICODE_CI",
        Seq(Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("AA"))), 2),
          Row(Row(Seq(Row("aa"))), 1, Row(Seq(Row("aa"))), 2)))
    )
    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x STRUCT<f:ARRAY<STRUCT<f:STRING COLLATE ${t.collation}>>>, " +
          s"i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES (named_struct('f', array(named_struct('f', 'aa'))), 1)")

        sql(s"CREATE TABLE $t2 (y STRUCT<f:ARRAY<STRUCT<f:STRING COLLATE ${t.collation}>>>, " +
          s"j int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES (named_struct('f', array(named_struct('f', 'AA'))), 2), " +
          s"(named_struct('f', array(named_struct('f', 'aa'))), 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // confirm that hash join is used instead of sort merge join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: ShuffledJoin => ()
          }.isEmpty
        )

        // Only if collation doesn't support binary equality, collation key should be injected.
        if (!CollationFactory.fetchCollation(t.collation).supportsBinaryEquality) {
          assert(queryPlan.toString().contains("collationkey"))
        } else {
          assert(!queryPlan.toString().contains("collationkey"))
        }
      }
    })
  }

  test("rewrite with collationkey should be an excludable rule") {
    val t1 = "T_1"
    val t2 = "T_2"
    val collation = "UTF8_LCASE"
    val collationRewriteJoinRule = "org.apache.spark.sql.catalyst.analysis.RewriteCollationJoin"
    withTable(t1, t2) {
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> collationRewriteJoinRule) {
        sql(s"CREATE TABLE $t1 (x STRING COLLATE $collation, i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES ('aa', 1)")

        sql(s"CREATE TABLE $t2 (y STRING COLLATE $collation, j int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES ('AA', 2), ('aa', 2)")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.y")
        checkAnswer(df, Seq(Row("aa", 1, "AA", 2), Row("aa", 1, "aa", 2)))

        val queryPlan = df.queryExecution.executedPlan

        // confirm that sort merge join is used instead of hash join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.isEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: SortMergeJoinExec => ()
          }.nonEmpty
        )
      }
    }
  }

  test("rewrite with collationkey shouldn't disrupt multiple join conditions") {
    val t1 = "T_1"
    val t2 = "T_2"

    case class HashMultiJoinTestCase[R](
      type1: String,
      type2: String,
      data1: String,
      data2: String,
      result: R
    )
    val testCases = Seq(
      HashMultiJoinTestCase("STRING COLLATE UTF8_BINARY", "INT",
        "'a', 0, 1", "'a', 0, 1", Row("a", 0, 1, "a", 0, 1)),
      HashMultiJoinTestCase("STRING COLLATE UTF8_BINARY", "STRING COLLATE UTF8_BINARY",
        "'a', 'a', 1", "'a', 'a', 1", Row("a", "a", 1, "a", "a", 1)),
      HashMultiJoinTestCase("STRING COLLATE UTF8_BINARY", "STRING COLLATE UTF8_LCASE",
        "'a', 'a', 1", "'a', 'A', 1", Row("a", "a", 1, "a", "A", 1)),
      HashMultiJoinTestCase("STRING COLLATE UTF8_LCASE", "STRING COLLATE UNICODE_CI",
        "'a', 'a', 1", "'A', 'A', 1", Row("a", "a", 1, "A", "A", 1))
    )

    testCases.foreach(t => {
      withTable(t1, t2) {
        sql(s"CREATE TABLE $t1 (x ${t.type1}, y ${t.type2}, i int) USING PARQUET")
        sql(s"INSERT INTO $t1 VALUES (${t.data1})")
        sql(s"CREATE TABLE $t2 (x ${t.type1}, y ${t.type2}, i int) USING PARQUET")
        sql(s"INSERT INTO $t2 VALUES (${t.data2})")

        val df = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.x = $t2.x AND $t1.y = $t2.y")
        checkAnswer(df, t.result)

        val queryPlan = df.queryExecution.executedPlan

        // confirm that hash join is used instead of sort merge join
        assert(
          collectFirst(queryPlan) {
            case _: HashJoin => ()
          }.nonEmpty
        )
        assert(
          collectFirst(queryPlan) {
            case _: SortMergeJoinExec => ()
          }.isEmpty
        )
      }
    })
  }

  test("hll sketch aggregate should respect collation") {
    case class HllSketchAggTestCase[R](c: String, result: R)
    val testCases = Seq(
      HllSketchAggTestCase("UTF8_BINARY", 4),
      HllSketchAggTestCase("UTF8_LCASE", 3),
      HllSketchAggTestCase("UNICODE", 4),
      HllSketchAggTestCase("UNICODE_CI", 3)
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.c) {
        val q = "SELECT hll_sketch_estimate(hll_sketch_agg(col)) FROM " +
          "VALUES ('a'), ('A'), ('b'), ('b'), ('c') tab(col)"
        val df = sql(q)
        checkAnswer(df, Seq(Row(t.result)))
      }
    })
  }

  test("cache table with collated columns") {
    val collations = Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI")
    val lazyOptions = Seq(false, true)

    for (
      collation <- collations;
      lazyTable <- lazyOptions
    ) {
      val lazyStr = if (lazyTable) "LAZY" else ""

      def checkCacheTable(values: String): Unit = {
        sql(s"CACHE $lazyStr TABLE tbl AS SELECT col FROM VALUES ($values) AS (col)")
        // Checks in-memory fetching code path.
        val all = sql("SELECT col FROM tbl")
        assert(all.queryExecution.executedPlan.collectFirst {
          case _: InMemoryTableScanExec => true
        }.nonEmpty)
        checkAnswer(all, Row("a"))
        // Checks column stats code path.
        checkAnswer(sql("SELECT col FROM tbl WHERE col = 'a'"), Row("a"))
        checkAnswer(sql("SELECT col FROM tbl WHERE col = 'b'"), Seq.empty)
      }

      withTable("tbl") {
        checkCacheTable(s"'a' COLLATE $collation")
      }
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
        withTable("tbl") {
          checkCacheTable("'a'")
        }
      }
    }
  }
}
