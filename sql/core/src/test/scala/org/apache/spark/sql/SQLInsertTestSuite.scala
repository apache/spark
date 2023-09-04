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

import org.apache.spark.{QueryContext, SparkConf, SparkNumberFormatException, SparkThrowable}
import org.apache.spark.sql.catalyst.expressions.Hex
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The base trait for SQL INSERT.
 */
trait SQLInsertTestSuite extends QueryTest with SQLTestUtils {

  import testImplicits._

  def format: String

  def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String],
      v1QueryContext: Array[QueryContext] = Array.empty,
      v2QueryContext: Array[QueryContext] = Array.empty): Unit

  def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Row,
      v2ExpectedAnswer: Row): Unit

  def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Seq[Row],
      v2ExpectedAnswer: Seq[Row]): Unit

  protected def createTable(
      table: String,
      cols: Seq[String],
      colTypes: Seq[String],
      partCols: Seq[String] = Nil): Unit = {
    val values = cols.zip(colTypes).map(tuple => tuple._1 + " " + tuple._2).mkString("(", ", ", ")")
    val partitionSpec = if (partCols.nonEmpty) {
      partCols.mkString("PARTITIONED BY (", ",", ")")
    } else ""
    sql(s"CREATE TABLE $table$values USING $format $partitionSpec")
  }

  protected def processInsert(
      tableName: String,
      input: DataFrame,
      cols: Seq[String] = Nil,
      partitionExprs: Seq[String] = Nil,
      overwrite: Boolean,
      byName: Boolean = false): Unit = {
    val tmpView = "tmp_view"
    val partitionList = if (partitionExprs.nonEmpty) {
      partitionExprs.mkString("PARTITION (", ",", ")")
    } else ""
    withTempView(tmpView) {
      input.createOrReplaceTempView(tmpView)
      val overwriteStr = if (overwrite) "OVERWRITE" else "INTO"
      val columnList = if (cols.nonEmpty && !byName) cols.mkString("(", ",", ")") else ""
      val byNameStr = if (byName) "BY NAME" else ""
      sql(
        s"INSERT $overwriteStr TABLE $tableName $partitionList $byNameStr " +
          s"$columnList SELECT * FROM $tmpView")
    }
  }

  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  test("insert with column list - follow table output order") {
    withTable("t1") {
      val df = Seq((1, 2L, "3")).toDF()
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - follow table output order + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2), Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df.selectExpr("c1", "c2", "c4"),
          cols.filterNot(_ == "c3"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - table output reorder") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((1, 2, 3)).toDF(cols: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }
  }

  test("insert with column list - by name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      processInsert("t1", df, overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert with column list - by name + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((4, 3, 2, 1)).toDF(cols.reverse: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df, overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert overwrite with column list - by name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      processInsert("t1", df, overwrite = false)
      verifyTable("t1", df.selectExpr(cols.reverse: _*))
      processInsert("t1", df, overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert overwrite with column list - by name + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((4, 3, 2, 1)).toDF(cols.reverse: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = false)
      verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = false)
      verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert by name: mismatch column name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val cols2 = Seq("x1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols2.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          processInsert("t1", df, overwrite = false, byName = true)
        },
        v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        v1Parameters = Map("tableName" -> "`spark_catalog`.`default`.`t1`",
          "extraColumns" -> "`x1`"),
        v2Parameters = Map("tableName" -> "`testcat`.`t1`", "extraColumns" -> "`x1`")
      )
      val df2 = Seq((3, 2, 1, 0)).toDF(Seq("c3", "c2", "c1", "c0"): _*)
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          processInsert("t1", df2, overwrite = false, byName = true)
        },
        v1ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v2ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v1Parameters = Map("tableName" -> "`spark_catalog`.`default`.`t1`",
          "tableColumns" -> "`c1`, `c2`, `c3`",
          "dataColumns" -> "`c3`, `c2`, `c1`, `c0`"),
        v2Parameters = Map("tableName" -> "`testcat`.`t1`",
          "tableColumns" -> "`c1`, `c2`, `c3`",
          "dataColumns" -> "`c3`, `c2`, `c1`, `c0`")
      )
    }
  }

  test("insert with column list - table output reorder + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2).reverse, Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1",
          df.selectExpr("c1", "c2", "c4"), Seq("c4", "c2", "c1"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c4", "c2", "c3", "c1"))
      }
    }
  }

  test("insert with column list - duplicated columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      checkError(
        exception = intercept[AnalysisException](
          sql(s"INSERT INTO t1 (c1, c2, c2) values(1, 2, 3)")),
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`c2`"))
    }
  }

  test("insert with column list - invalid columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      checkError(
        exception =
          intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c4) values(1, 2, 3)")),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map("objectName" -> "`c4`", "proposal" -> "`c1`, `c2`, `c3`"),
        context = ExpectedContext(
          fragment = "INSERT INTO t1 (c1, c2, c4)", start = 0, stop = 26
        ))
    }
  }

  test("insert with column list - mismatched column list size") {
    def test: Unit = {
      withTable("t1") {
        val cols = Seq("c1", "c2", "c3")
        createTable("t1", cols, Seq("int", "long", "string"))
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO t1 (c1, c2) values(1, 2, 3)")
          },
          sqlState = None,
          errorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          parameters = Map(
            "tableName" -> ".*`t1`",
            "tableColumns" -> "`c1`, `c2`",
            "dataColumns" -> "`col1`, `col2`, `col3`"),
          matchPVals = true
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO t1 (c1, c2, c3) values(1, 2)")
          },
          sqlState = None,
          errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          parameters = Map(
            "tableName" -> ".*`t1`",
            "tableColumns" -> "`c1`, `c2`, `c3`",
            "dataColumns" -> "`col1`, `col2`"),
          matchPVals = true
        )
      }
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
      test
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      test
    }
  }

  test("SPARK-34223: static partition with null raise NPE") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      sql("INSERT OVERWRITE t PARTITION (c=null) VALUES ('1')")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("SPARK-38336 INSERT INTO statements with tables with default columns: positive tests") {
    // When the INSERT INTO statement provides fewer values than expected, NULL values are appended
    // in their place.
    withTable("t") {
      sql("create table t(i boolean, s bigint) using parquet")
      sql("insert into t(i) values(true)")
      checkAnswer(spark.table("t"), Row(true, null))
    }
    // The default value for the DEFAULT keyword is the NULL literal.
    withTable("t") {
      sql("create table t(i boolean, s bigint) using parquet")
      sql("insert into t values(true, default)")
      checkAnswer(spark.table("t"), Row(true, null))
    }
    // There is a complex expression in the default value.
    withTable("t") {
      sql("create table t(i boolean, s string default concat('abc', 'def')) using parquet")
      sql("insert into t values(true, default)")
      checkAnswer(spark.table("t"), Row(true, "abcdef"))
    }
    // The default value parses correctly and the provided value type is different but coercible.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      sql("insert into t(i) values(false)")
      checkAnswer(spark.table("t"), Row(false, 42L))
    }
    // There are two trailing default values referenced implicitly by the INSERT INTO statement.
    withTable("t") {
      sql("create table t(i int, s bigint default 42, x bigint default 43) using parquet")
      sql("insert into t(i) values(1)")
      checkAnswer(sql("select s + x from t where i = 1"), Seq(85L).map(i => Row(i)))
    }
    withTable("t") {
      sql("create table t(i int, s bigint default 42, x bigint) using parquet")
      sql("insert into t(i) values(1)")
      checkAnswer(spark.table("t"), Row(1, 42L, null))
    }
    // The table has a partitioning column and a default value is injected.
    withTable("t") {
      sql("create table t(i boolean, s bigint, q int default 42) using parquet partitioned by (i)")
      sql("insert into t partition(i='true') values(5, default)")
      checkV1AndV2Answer(spark.table("t"), Row(5, 42, true), Row(true, 5, 42))
    }
    // The table has a partitioning column and a default value is added per an explicit reference.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet partitioned by (i)")
      sql("insert into t partition(i='true') (s) values(default)")
      checkV1AndV2Answer(spark.table("t"), Row(42L, true), Row(true, 42L))
    }
    // The default value parses correctly as a constant but non-literal expression.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 41 + 1) using parquet")
      sql("insert into t values(false, default)")
      checkAnswer(spark.table("t"), Row(false, 42L))
    }
    // Explicit defaults may appear in different positions within the inline table provided as input
    // to the INSERT INTO statement.
    withTable("t") {
      sql("create table t(i boolean default false, s bigint default 42) using parquet")
      sql("insert into t(i, s) values(false, default), (default, 42)")
      checkAnswer(spark.table("t"), Seq(Row(false, 42L), Row(false, 42L)))
    }
    // There is an explicit default value provided in the INSERT INTO statement in the VALUES,
    // with an alias over the VALUES.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      sql("insert into t select * from values (false, default) as tab(col, other)")
      checkAnswer(spark.table("t"), Row(false, 42L))
    }
    // The explicit default value arrives first before the other value.
    withTable("t") {
      sql("create table t(i boolean default false, s bigint) using parquet")
      sql("insert into t values (default, 43)")
      checkAnswer(spark.table("t"), Row(false, 43L))
    }
    // The 'create table' statement provides the default parameter first.
    withTable("t") {
      sql("create table t(i boolean default false, s bigint) using parquet")
      sql("insert into t values (default, 43)")
      checkAnswer(spark.table("t"), Row(false, 43L))
    }
    // The explicit default value is provided in the wrong order (first instead of second), but
    // this is OK because the provided default value evaluates to literal NULL.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      sql("insert into t values (default, 43)")
      checkAnswer(spark.table("t"), Row(null, 43L))
    }
    // There is an explicit default value provided in the INSERT INTO statement as a SELECT.
    // This is supported.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      sql("insert into t select false, default")
      checkAnswer(spark.table("t"), Row(false, 42L))
    }
    // There is a complex query plan in the SELECT query in the INSERT INTO statement.
    withTable("t") {
      sql("create table t(i boolean default false, s bigint default 42) using parquet")
      sql("insert into t select col, count(*) from values (default, default) " +
        "as tab(col, other) group by 1")
      checkAnswer(spark.table("t"), Row(false, 1))
    }
    // The explicit default reference resolves successfully with nested table subqueries.
    withTable("t") {
      sql("create table t(i boolean default false, s bigint) using parquet")
      sql("insert into t select * from (select * from values(default, 42))")
      checkAnswer(spark.table("t"), Row(false, 42L))
    }
    // There are three column types exercising various combinations of implicit and explicit
    // default column value references in the 'insert into' statements. Note these tests depend on
    // enabling the configuration to use NULLs for missing DEFAULT column values.
    for (useDataFrames <- Seq(false, true)) {
      withTable("t1", "t2") {
        sql("create table t1(j int, s bigint default 42, x bigint default 43) using parquet")
        if (useDataFrames) {
          Seq((1, 42, 43)).toDF.write.insertInto("t1")
          Seq((2, 42, 43)).toDF.write.insertInto("t1")
          Seq((3, 42, 43)).toDF.write.insertInto("t1")
          Seq((4, 44, 43)).toDF.write.insertInto("t1")
          Seq((5, 44, 43)).toDF.write.insertInto("t1")
        } else {
          sql("insert into t1(j) values(1)")
          sql("insert into t1(j, s) values(2, default)")
          sql("insert into t1(j, s, x) values(3, default, default)")
          sql("insert into t1(j, s) values(4, 44)")
          sql("insert into t1(j, s, x) values(5, 44, 45)")
        }
        sql("create table t2(j int, s bigint default 42, x bigint default 43) using parquet")
        if (useDataFrames) {
          spark.table("t1").where("j = 1").write.insertInto("t2")
          spark.table("t1").where("j = 2").write.insertInto("t2")
          spark.table("t1").where("j = 3").write.insertInto("t2")
          spark.table("t1").where("j = 4").write.insertInto("t2")
          spark.table("t1").where("j = 5").write.insertInto("t2")
        } else {
          sql("insert into t2(j) select j from t1 where j = 1")
          sql("insert into t2(j, s) select j, default from t1 where j = 2")
          sql("insert into t2(j, s, x) select j, default, default from t1 where j = 3")
          sql("insert into t2(j, s) select j, s from t1 where j = 4")
          sql("insert into t2(j, s, x) select j, s, default from t1 where j = 5")
        }
        checkAnswer(
          spark.table("t2"),
          Row(1, 42L, 43L) ::
            Row(2, 42L, 43L) ::
            Row(3, 42L, 43L) ::
            Row(4, 44L, 43L) ::
            Row(5, 44L, 43L) :: Nil)
      }
    }
  }

  test("SPARK-38336 INSERT INTO statements with tables with default columns: negative tests") {
    // The default value fails to analyze.
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("create table t(i boolean, s bigint default badvalue) using parquet")
        },
        errorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "badvalue"))
    }
    // The default value analyzes to a table not in the catalog.
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("create table t(i boolean, s bigint default (select min(x) from badtable)) " +
            "using parquet")
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "(select min(x) from badtable)"))
    }
    // The default value parses but refers to a table from the catalog.
    withTable("t", "other") {
      sql("create table other(x string) using parquet")
      checkError(
        exception = intercept[AnalysisException] {
          sql("create table t(i boolean, s bigint default (select min(x) from other)) " +
            "using parquet")
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "(select min(x) from other)"))
    }
    // The default value has an explicit alias. It fails to evaluate when inlined into the VALUES
    // list at the INSERT INTO time.
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("create table t(i boolean default (select false as alias), s bigint) using parquet")
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`i`",
          "defaultValue" -> "(select false as alias)"))
    }
    // Explicit default values may not participate in complex expressions in the VALUES list.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values(false, default + 1)")
        },
        errorClass = "DEFAULT_PLACEMENT_INVALID",
        parameters = Map.empty
      )
    }
    // Explicit default values may not participate in complex expressions in the SELECT query.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t select false, default + 1")
        },
        errorClass = "DEFAULT_PLACEMENT_INVALID",
        parameters = Map.empty
      )
    }
    // Explicit default values have a reasonable error path if the table is not found.
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values(false, default)")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`t`"),
        context = ExpectedContext("t", 12, 12)
      )
    }
    // The default value parses but the type is not coercible.
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("create table t(i boolean, s bigint default false) using parquet")
        },
        errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "expectedType" -> "\"BIGINT\"",
          "defaultValue" -> "false",
          "actualType" -> "\"BOOLEAN\""))
    }
    // The number of columns in the INSERT INTO statement is greater than the number of columns in
    // the table.
    withTable("t") {
      sql("create table num_data(id int, val decimal(38,10)) using parquet")
      sql("create table t(id1 int, int2 int, result decimal(38,10)) using parquet")
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          sql("insert into t select t1.id, t2.id, t1.val, t2.val, t1.val * t2.val " +
            "from num_data t1, num_data t2")
        },
        v1ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v2ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v1Parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`id1`, `int2`, `result`",
          "dataColumns" -> "`id`, `id`, `val`, `val`, `(val * val)`"),
        v2Parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "tableColumns" -> "`id1`, `int2`, `result`",
          "dataColumns" -> "`id`, `id`, `val`, `val`, `(val * val)`"))
    }
    // The default value is disabled per configuration.
    withTable("t") {
      withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
        checkError(
          exception = intercept[ParseException] {
            sql("create table t(i boolean, s bigint default 42L) using parquet")
          },
          errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION",
          parameters = Map.empty,
          context = ExpectedContext("s bigint default 42L", 26, 45)
        )
      }
    }
    // The table has a partitioning column with a default value; this is not allowed.
    withTable("t") {
      sql("create table t(i boolean default true, s bigint, q int default 42) " +
        "using parquet partitioned by (i)")
      checkError(
        exception = intercept[ParseException] {
          sql("insert into t partition(i=default) values(5, default)")
        },
        errorClass = "REF_DEFAULT_VALUE_IS_NOT_ALLOWED_IN_PARTITION",
        parameters = Map.empty,
        context = ExpectedContext(
          fragment = "partition(i=default)",
          start = 14,
          stop = 33))
    }
    // The configuration option to append missing NULL values to the end of the INSERT INTO
    // statement is not enabled.
    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
      withTable("t") {
        sql("create table t(i boolean, s bigint) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t values(true)")
          },
          v1ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          v2ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "tableColumns" -> "`i`, `s`",
            "dataColumns" -> "`col1`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "tableColumns" -> "`i`, `s`",
            "dataColumns" -> "`col1`"))
      }
    }
  }

  test("SPARK-38795 INSERT INTO with user specified columns and defaults: positive tests") {
    Seq(
      "insert into t (i, s) values (true, default)",
      "insert into t (s, i) values (default, true)",
      "insert into t (i) values (true)",
      "insert into t (i) values (default)",
      "insert into t (s) values (default)",
      "insert into t (s) select default from (select 1)",
      "insert into t (i) select true from (select 1)"
    ).foreach { insert =>
      withTable("t") {
        sql("create table t(i boolean default true, s bigint default 42) using parquet")
        sql(insert)
        checkAnswer(spark.table("t"), Row(true, 42L))
      }
    }
    // The table is partitioned and we insert default values with explicit column names.
    withTable("t") {
      sql("create table t(i boolean, s bigint default 4, q int default 42) using parquet " +
        "partitioned by (i)")
      sql("insert into t partition(i='true') (s) values(5)")
      sql("insert into t partition(i='false') (q) select 43")
      sql("insert into t partition(i='false') (q) select default")
      checkV1AndV2Answer(df = spark.table("t"),
        v1ExpectedAnswer = Seq(Row(5, 42, true),
          Row(4, 43, false),
          Row(4, 42, false)),
        v2ExpectedAnswer = Seq(Row(true, 5, 42),
          Row(false, 4, 43),
          Row(false, 4, 42)))
    }
    // If no explicit DEFAULT value is available when the INSERT INTO statement provides fewer
    // values than expected, NULL values are appended in their place.
    withTable("t") {
      sql("create table t(i boolean, s bigint) using parquet")
      sql("insert into t (i) values (true)")
      checkAnswer(spark.table("t"), Row(true, null))
    }
    withTable("t") {
      sql("create table t(i boolean default true, s bigint) using parquet")
      sql("insert into t (i) values (default)")
      checkAnswer(spark.table("t"), Row(true, null))
    }
    withTable("t") {
      sql("create table t(i boolean, s bigint default 42) using parquet")
      sql("insert into t (s) values (default)")
      checkAnswer(spark.table("t"), Row(null, 42L))
    }
    withTable("t") {
      sql("create table t(i boolean, s bigint, q int) using parquet partitioned by (i)")
      sql("insert into t partition(i='true') (s) values(5)")
      sql("insert into t partition(i='false') (q) select 43")
      sql("insert into t partition(i='false') (q) select default")
      checkV1AndV2Answer(df = spark.table("t"),
        v1ExpectedAnswer = Seq(Row(5, null, true),
          Row(null, 43, false),
          Row(null, null, false)),
        v2ExpectedAnswer = Seq(Row(true, 5, null),
          Row(false, null, 43),
          Row(false, null, null)))
    }
  }

  test("SPARK- 38795 INSERT INTO with user specified columns and defaults: negative tests") {
    // The missing columns in these INSERT INTO commands do not have explicit default values.
    withTable("t") {
      sql("create table t(i boolean, s bigint, q int default 43) using parquet")
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          sql("insert into t (i, q) select true from (select 1)")
        },
        v1ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        v2ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        v1Parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`i`, `q`",
          "dataColumns" -> "`true`"),
        v2Parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "tableColumns" -> "`i`, `q`",
          "dataColumns" -> "`true`"))
    }
    // When the USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES configuration is disabled, and no
    // explicit DEFAULT value is available when the INSERT INTO statement provides fewer
    // values than expected, the INSERT INTO command fails to execute.
    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
      withTable("t") {
        sql("create table t(i boolean, s bigint) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t (i) values (true)")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`s`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`s`"))
      }
      withTable("t") {
        sql("create table t(i boolean default true, s bigint) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t (i) values (default)")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`s`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`s`"))
      }
      withTable("t") {
        sql("create table t(i boolean, s bigint default 42) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t (s) values (default)")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`i`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`i`"))
      }
      withTable("t") {
        sql("create table t(i boolean, s bigint, q int) using parquet partitioned by (i)")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t partition(i='true') (s) values(5)")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`q`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`q`"))
      }
      withTable("t") {
        sql("create table t(i boolean, s bigint, q int) using parquet partitioned by (i)")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t partition(i='false') (q) select 43")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`s`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`s`"))
      }
      withTable("t") {
        sql("create table t(i boolean, s bigint, q int) using parquet partitioned by (i)")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("insert into t partition(i='false') (q) select default")
          },
          v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          v1Parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`s`"),
          v2Parameters = Map(
            "tableName" -> "`testcat`.`t`",
            "colName" -> "`s`"))
      }
    }
    // When the CASE_SENSITIVE configuration is enabled, then using different cases for the required
    // and provided column names results in an analysis error.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("t") {
        sql("create table t(i boolean default true, s bigint default 42) using parquet")
        checkError(
          exception =
            intercept[AnalysisException](sql("insert into t (I) select true from (select 1)")),
          errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = None,
          parameters = Map("objectName" -> "`I`", "proposal" -> "`i`, `s`"),
          context = ExpectedContext(
            fragment = "insert into t (I)", start = 0, stop = 16))
      }
    }
  }

  test("SPARK-38811 INSERT INTO on columns added with ALTER TABLE ADD COLUMNS: Positive tests") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "parquet, ") {
      // There is a complex expression in the default value.
      val createTableBooleanCol = "create table t(i boolean) using parquet"
      val createTableIntCol = "create table t(i int) using parquet"
      withTable("t") {
        sql(createTableBooleanCol)
        sql("alter table t add column s string default concat('abc', 'def')")
        sql("insert into t values(true, default)")
        checkAnswer(spark.table("t"), Row(true, "abcdef"))
      }
      // There are two trailing default values referenced implicitly by the INSERT INTO statement.
      withTable("t") {
        sql(createTableIntCol)
        sql("alter table t add column s bigint default 42")
        sql("alter table t add column x bigint default 43")
        sql("insert into t(i) values(1)")
        checkAnswer(spark.table("t"), Row(1, 42, 43))
      }
      // There are two trailing default values referenced implicitly by the INSERT INTO statement.
      withTable("t") {
        sql(createTableIntCol)
        sql("alter table t add columns s bigint default 42, x bigint default 43")
        sql("insert into t(i) values(1)")
        checkAnswer(spark.table("t"), Row(1, 42, 43))
      }
      withTable("t") {
        sql(createTableIntCol)
        sql("alter table t add column s bigint default 42")
        sql("alter table t add column x bigint")
        sql("insert into t(i) values(1)")
        checkAnswer(spark.table("t"), Row(1, 42, null))
      }
      // The table has a partitioning column and a default value is injected.
      withTable("t") {
        sql("create table t(i boolean, s bigint) using parquet partitioned by (i)")
        sql("alter table t add column q int default 42")
        sql("insert into t partition(i='true') values(5, default)")
        checkV1AndV2Answer(spark.table("t"), Row(5, 42, true), Row(true, 5, 42))
      }
      // The default value parses correctly as a constant but non-literal expression.
      withTable("t") {
        sql(createTableBooleanCol)
        sql("alter table t add column s bigint default 41 + 1")
        sql("insert into t(i) values(default)")
        checkAnswer(spark.table("t"), Row(null, 42))
      }
      // Explicit defaults may appear in different positions within the inline table provided as
      // input to the INSERT INTO statement.
      withTable("t") {
        sql("create table t(i boolean default false) using parquet")
        sql("alter table t add column s bigint default 42")
        sql("insert into t values(false, default), (default, 42)")
        checkAnswer(spark.table("t"), Seq(Row(false, 42), Row(false, 42)))
      }
      // There is an explicit default value provided in the INSERT INTO statement in the VALUES,
      // with an alias over the VALUES.
      withTable("t") {
        sql(createTableBooleanCol)
        sql("alter table t add column s bigint default 42")
        sql("insert into t select * from values (false, default) as tab(col, other)")
        checkAnswer(spark.table("t"), Row(false, 42))
      }
      // The explicit default value is provided in the wrong order (first instead of second), but
      // this is OK because the provided default value evaluates to literal NULL.
      withTable("t") {
        sql(createTableBooleanCol)
        sql("alter table t add column s bigint default 42")
        sql("insert into t values (default, 43)")
        checkAnswer(spark.table("t"), Row(null, 43))
      }
      // There is an explicit default value provided in the INSERT INTO statement as a SELECT.
      // This is supported.
      withTable("t") {
        sql(createTableBooleanCol)
        sql("alter table t add column s bigint default 42")
        sql("insert into t select false, default")
        checkAnswer(spark.table("t"), Row(false, 42))
      }
      // There is a complex query plan in the SELECT query in the INSERT INTO statement.
      withTable("t") {
        sql("create table t(i boolean default false) using parquet")
        sql("alter table t add column s bigint default 42")
        sql("insert into t select col, count(*) from values (default, default) " +
          "as tab(col, other) group by 1")
        checkAnswer(spark.table("t"), Row(false, 1))
      }
      // There are three column types exercising various combinations of implicit and explicit
      // default column value references in the 'insert into' statements. Note these tests depend on
      // enabling the configuration to use NULLs for missing DEFAULT column values.
      withTable("t1", "t2") {
        sql("create table t1(j int) using parquet")
        sql("alter table t1 add column s bigint default 42")
        sql("alter table t1 add column x bigint default 43")
        sql("insert into t1(j) values(1)")
        sql("insert into t1(j, s) values(2, default)")
        sql("insert into t1(j, s, x) values(3, default, default)")
        sql("insert into t1(j, s) values(4, 44)")
        sql("insert into t1(j, s, x) values(5, 44, 45)")
        sql("create table t2(j int) using parquet")
        sql("alter table t2 add columns s bigint default 42, x bigint default 43")
        sql("insert into t2(j) select j from t1 where j = 1")
        sql("insert into t2(j, s) select j, default from t1 where j = 2")
        sql("insert into t2(j, s, x) select j, default, default from t1 where j = 3")
        sql("insert into t2(j, s) select j, s from t1 where j = 4")
        sql("insert into t2(j, s, x) select j, s, default from t1 where j = 5")
        checkAnswer(
          spark.table("t2"),
          Row(1, 42L, 43L) ::
            Row(2, 42L, 43L) ::
            Row(3, 42L, 43L) ::
            Row(4, 44L, 43L) ::
            Row(5, 44L, 43L) :: Nil)
      }
    }
  }

  test("SPARK-38811 INSERT INTO on columns added with ALTER TABLE ADD COLUMNS: Negative tests") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "parquet, ") {
      // The default value fails to analyze.
      withTable("t") {
        sql("create table t(i boolean) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s bigint default badvalue")
          },
          v1ErrorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
          v2ErrorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
          v1Parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "defaultValue" -> "badvalue"),
          v2Parameters = Map(
            "statement" -> "ALTER TABLE",
            "colName" -> "`s`",
            "defaultValue" -> "badvalue"))
      }
      // The default value analyzes to a table not in the catalog.
      withTable("t") {
        sql("create table t(i boolean) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s bigint default (select min(x) from badtable)")
          },
          v1ErrorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
          v2ErrorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
          v1Parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "defaultValue" -> "(select min(x) from badtable)"),
          v2Parameters = Map(
            "statement" -> "ALTER TABLE",
            "colName" -> "`s`",
            "defaultValue" -> "(select min(x) from badtable)"))
      }
      // The default value parses but refers to a table from the catalog.
      withTable("t", "other") {
        sql("create table other(x string) using parquet")
        sql("create table t(i boolean) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s bigint default (select min(x) from other)")
          },
          v1ErrorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
          v2ErrorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
          v1Parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "defaultValue" -> "(select min(x) from other)"),
          v2Parameters = Map(
            "statement" -> "ALTER TABLE",
            "colName" -> "`s`",
            "defaultValue" -> "(select min(x) from other)"))
      }
      // The default value parses but the type is not coercible.
      withTable("t") {
        sql("create table t(i boolean) using parquet")
        checkV1AndV2Error(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s bigint default false")
          },
          v1ErrorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          v2ErrorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          v1Parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "expectedType" -> "\"BIGINT\"",
            "defaultValue" -> "false",
            "actualType" -> "\"BOOLEAN\""),
          v2Parameters = Map(
            "statement" -> "ALTER TABLE",
            "colName" -> "`s`",
            "expectedType" -> "\"BIGINT\"",
            "defaultValue" -> "false",
            "actualType" -> "\"BOOLEAN\""))
      }
      // The default value is disabled per configuration.
      withTable("t") {
        withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
          sql("create table t(i boolean) using parquet")
          checkError(
            exception = intercept[ParseException] {
              sql("alter table t add column s bigint default 42L")
            },
            errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION",
            parameters = Map.empty,
            context = ExpectedContext(
              fragment = "s bigint default 42L",
              start = 25,
              stop = 44)
          )
        }
      }
    }
  }

  test("INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them") {
    case class Config(
        sqlConf: Option[(String, String)],
        useDataFrames: Boolean = false)
    def runTest(dataSource: String, config: Config): Unit = {
      withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$dataSource, ") {
        def insertIntoT(): Unit = {
          sql("insert into t(a, i) values('xyz', 42)")
        }

        def withTableT(f: => Unit): Unit = {
          sql(s"create table t(a string, i int) using $dataSource")
          insertIntoT
          withTable("t") {
            f
          }
        }
        // Positive tests:
        // Adding a column with a valid default value into a table containing existing data works
        // successfully. Querying data from the altered table returns the new value.
        withTableT {
          sql("alter table t add column (s string default concat('abc', 'def'))")
          checkAnswer(spark.table("t"), Row("xyz", 42, "abcdef"))
          checkAnswer(sql("select i, s from t"), Row(42, "abcdef"))
          // Now alter the column to change the default value. This still returns the previous
          // value, not the new value, since the behavior semantics are the same as if the first
          // command had performed a backfill of the new default value in the existing rows.
          sql("alter table t alter column s set default concat('ghi', 'jkl')")
          checkAnswer(sql("select i, s from t"), Row(42, "abcdef"))
        }
        // Adding a column with a default value and then inserting explicit NULL values works.
        // Querying data back from the table differentiates between the explicit NULL values and
        // default values.
        withTableT {
          sql("alter table t add column (s string default concat('abc', 'def'))")
          if (config.useDataFrames) {
            Seq((null, null, null)).toDF.write.insertInto("t")
          } else {
            sql("insert into t values(null, null, null)")
          }
          sql("alter table t add column (x boolean default true)")
          val insertedSColumn = null
          checkAnswer(spark.table("t"),
            Seq(
              Row("xyz", 42, "abcdef", true),
              Row(null, null, insertedSColumn, true)))
          checkAnswer(sql("select i, s, x from t"),
            Seq(
              Row(42, "abcdef", true),
              Row(null, insertedSColumn, true)))
        }
        // Adding two columns where only the first has a valid default value works successfully.
        // Querying data from the altered table returns the default value as well as NULL for the
        // second column.
        withTableT {
          sql("alter table t add column (s string default concat('abc', 'def'))")
          sql("alter table t add column (x string)")
          checkAnswer(spark.table("t"), Row("xyz", 42, "abcdef", null))
          checkAnswer(sql("select i, s, x from t"), Row(42, "abcdef", null))
        }
        // Test other supported data types.
        withTableT {
          sql("alter table t add columns (" +
            "s boolean default true, " +
            "t byte default cast(null as byte), " +
            "u short default cast(42 as short), " +
            "v float default 0, " +
            "w double default 0, " +
            "x date default cast('2021-01-02' as date), " +
            "y timestamp default cast('2021-01-02 01:01:01' as timestamp), " +
            "z timestamp_ntz default cast('2021-01-02 01:01:01' as timestamp_ntz), " +
            "a1 timestamp_ltz default cast('2021-01-02 01:01:01' as timestamp_ltz), " +
            "a2 decimal(5, 2) default 123.45," +
            "a3 bigint default 43," +
            "a4 smallint default cast(5 as smallint)," +
            "a5 tinyint default cast(6 as tinyint))")
          insertIntoT()
          // Manually inspect the result row values rather than using the 'checkAnswer' helper
          // method in order to ensure the values' correctness while avoiding minor type
          // incompatibilities.
          val result: Array[Row] =
          sql("select s, t, u, v, w, x, y, z, a1, a2, a3, a4, a5 from t").collect()
          for (row <- result) {
            assert(row.length == 13)
            assert(row(0) == true)
            assert(row(1) == null)
            assert(row(2) == 42)
            assert(row(3) == 0.0f)
            assert(row(4) == 0.0d)
            assert(row(5).toString == "2021-01-02")
            assert(row(6).toString == "2021-01-02 01:01:01.0")
            assert(row(7).toString.startsWith("2021-01-02"))
            assert(row(8).toString == "2021-01-02 01:01:01.0")
            assert(row(9).toString == "123.45")
            assert(row(10) == 43L)
            assert(row(11) == 5)
            assert(row(12) == 6)
          }
        }
      }

      // This represents one test configuration over a data source.
      case class TestCase(
          dataSource: String,
          configs: Seq[Config])
      // Run the test several times using each configuration.
      Seq(
        TestCase(
          dataSource = "csv",
          Seq(
            Config(
              None),
            Config(
              Some(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "false")))),
        TestCase(
          dataSource = "json",
          Seq(
            Config(
              None),
            Config(
              Some(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "false")))),
        TestCase(
          dataSource = "orc",
          Seq(
            Config(
              None),
            Config(
              Some(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false")))),
        TestCase(
          dataSource = "parquet",
          Seq(
            Config(
              None),
            Config(
              Some(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false"))))
      ).foreach { testCase: TestCase =>
        testCase.configs.foreach { config: Config =>
          // Run the test twice, once using SQL for the INSERT operations and again using
          // DataFrames.
          for (useDataFrames <- Seq(false, true)) {
            config.sqlConf.map { kv: (String, String) =>
              withSQLConf(kv) {
                // Run the test with the pair of custom SQLConf values.
                runTest(testCase.dataSource, config.copy(useDataFrames = useDataFrames))
              }
            }.getOrElse {
              // Run the test with default settings.
              runTest(testCase.dataSource, config.copy(useDataFrames = useDataFrames))
            }
          }
        }
      }
    }
  }

  test("SPARK-39985 Enable implicit DEFAULT column values in inserts from DataFrames") {
    // Negative test: explicit column "default" references are not supported in write operations
    // from DataFrames: since the operators are resolved one-by-one, any .select referring to
    // "default" generates a "column not found" error before any following .insertInto.
    withTable("t") {
      sql(s"create table t(a string, i int default 42) using parquet")
      checkError(
        exception = intercept[AnalysisException] {
          Seq("xyz").toDF.select("value", "default").write.insertInto("t")
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`default`", "proposal" -> "`value`"))
    }
  }

  test("SPARK-38838 INSERT INTO with defaults set by ALTER TABLE ALTER COLUMN: positive tests") {
    withTable("t") {
      sql("create table t(i boolean, s string, k bigint) using parquet")
      // The default value for the DEFAULT keyword is the NULL literal.
      sql("insert into t values(true, default, default)")
      // There is a complex expression in the default value.
      sql("alter table t alter column s set default concat('abc', 'def')")
      sql("insert into t values(true, default, default)")
      // The default value parses correctly and the provided value type is different but coercible.
      sql("alter table t alter column k set default 42")
      sql("insert into t values(true, default, default)")
      // After dropping the default, inserting more values should add NULLs.
      sql("alter table t alter column k drop default")
      sql("insert into t values(true, default, default)")
      checkAnswer(spark.table("t"),
        Seq(
          Row(true, null, null),
          Row(true, "abcdef", null),
          Row(true, "abcdef", 42),
          Row(true, "abcdef", null)
        ))
    }
  }

  test("SPARK-38838 INSERT INTO with defaults set by ALTER TABLE ALTER COLUMN: negative tests") {
    val createTable = "create table t(i boolean, s bigint) using parquet"
    withTable("t") {
      sql(createTable)
      // The default value fails to analyze.
      checkError(
        exception = intercept[AnalysisException] {
          sql("alter table t alter column s set default badvalue")
        },
        errorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
        parameters = Map(
          "statement" -> "ALTER TABLE ALTER COLUMN",
          "colName" -> "`s`",
          "defaultValue" -> "badvalue"))
      // The default value analyzes to a table not in the catalog.
      checkError(
        exception = intercept[AnalysisException] {
          sql("alter table t alter column s set default (select min(x) from badtable)")
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "ALTER TABLE ALTER COLUMN",
          "colName" -> "`s`",
          "defaultValue" -> "(select min(x) from badtable)"))
      // The default value has an explicit alias. It fails to evaluate when inlined into the VALUES
      // list at the INSERT INTO time.
      checkError(
        exception = intercept[AnalysisException] {
          sql("alter table t alter column s set default (select 42 as alias)")
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "ALTER TABLE ALTER COLUMN",
          "colName" -> "`s`",
          "defaultValue" -> "(select 42 as alias)"))
      // The default value parses but the type is not coercible.
      checkError(
        exception = intercept[AnalysisException] {
          sql("alter table t alter column s set default false")
        },
        errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
        parameters = Map(
          "statement" -> "ALTER TABLE ALTER COLUMN",
          "colName" -> "`s`",
          "expectedType" -> "\"BIGINT\"",
          "defaultValue" -> "false",
          "actualType" -> "\"BOOLEAN\""))
      // The default value is disabled per configuration.
      withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
        val sqlText = "alter table t alter column s set default 41 + 1"
        checkError(
          exception = intercept[ParseException] {
            sql(sqlText)
          },
          errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION",
          parameters = Map.empty,
          context = ExpectedContext(
            fragment = sqlText,
            start = 0,
            stop = 46))
      }
    }
    // DataSource V2 supported set a default value for a partitioning column.
    if (format != "foo") {
      // Attempting to set a default value for a partitioning column is not allowed.
      withTable("t") {
        sql("create table t(i boolean, s bigint, q int default 42) using parquet" +
          " partitioned by (i)")
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t alter column i set default false")
          },
          errorClass = "_LEGACY_ERROR_TEMP_1246",
          parameters = Map("name" -> "i", "fieldNames" -> "[`s`, `q`]"))
      }
    }
  }

  test("SPARK-40001 JSON DEFAULT columns = JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE off") {
    // DataSource V2 doesn't supported json generator.
    if (format != "foo") {
      // Check that the JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE config overrides the
      // JSON_GENERATOR_IGNORE_NULL_FIELDS config.
      withSQLConf(SQLConf.JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE.key -> "true",
        SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
        withTable("t") {
          sql("create table t (a int default 42) using json")
          sql("insert into t values (null)")
          checkAnswer(spark.table("t"), Row(null))
        }
      }
      withSQLConf(SQLConf.JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE.key -> "false",
        SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
        withTable("t") {
          sql("create table t (a int default 42) using json")
          sql("insert into t values (null)")
          checkAnswer(spark.table("t"), Row(42))
        }
      }
    }
  }

  test("SPARK-39359 Restrict DEFAULT columns to allowlist of supported data source types") {
    // DataSource V2 doesn't check allowlist at now
    if (format != "foo") {
      withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "csv,json,orc") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"create table t(a string default 'abc') using parquet")
          },
          errorClass = "_LEGACY_ERROR_TEMP_1345",
          parameters = Map("statementType" -> "CREATE TABLE", "dataSource" -> "parquet"))
        withTable("t") {
          sql(s"create table t(a string, b int) using parquet")
          checkV1AndV2Error(
            exception = intercept[AnalysisException] {
              sql("alter table t add column s bigint default 42")
            },
            v1ErrorClass = "_LEGACY_ERROR_TEMP_1345",
            v1Parameters = Map(
              "statementType" -> "ALTER TABLE ADD COLUMNS",
              "dataSource" -> "parquet"),
            v2ErrorClass = "_LEGACY_ERROR_TEMP_1345",
            v2Parameters = Map(
              "statementType" -> "ALTER TABLE",
              "dataSource" -> ""))
        }
      }
    }
  }

  test("SPARK-39557 INSERT INTO statements with tables with array defaults") {
    // Positive tests: array types are supported as default values.
    case class Config(
        dataSource: String,
        useDataFrames: Boolean = false)
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        useDataFrames = true),
      Config(
        "orc"),
      Config(
        "orc",
        useDataFrames = true)).foreach { config =>
      withTable("t") {
        withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"${config.dataSource}, ") {
          sql(s"create table t(i boolean) using ${config.dataSource}")
          if (config.useDataFrames) {
            Seq(false).toDF.write.insertInto("t")
          } else {
            sql("insert into t select false")
          }
          sql("alter table t add column s array<int> default array(1, 2)")
          checkAnswer(spark.table("t"), Row(false, Seq(1, 2)))
        }
      }
    }
    // Negative tests: provided array element types must match their corresponding DEFAULT
    // declarations, if applicable.
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        true)).foreach { config =>
      withTable("t") {
        withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"${config.dataSource}, ") {
          sql(s"create table t(i boolean) using ${config.dataSource}")
          if (config.useDataFrames) {
            Seq((false)).toDF.write.insertInto("t")
          } else {
            sql("insert into t select false")
          }
          checkError(
            exception = intercept[AnalysisException] {
              sql("alter table t add column s array<int> default array('abc', 'def')")
            },
            errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
            parameters = Map(
              "statement" -> "ALTER TABLE ADD COLUMNS",
              "colName" -> "`s`",
              "expectedType" -> "\"ARRAY<INT>\"",
              "defaultValue" -> "array('abc', 'def')",
              "actualType" -> "\"ARRAY<STRING>\""))
        }
      }
    }
  }

  test("SPARK-39557 INSERT INTO statements with tables with struct defaults") {
    // Positive tests: struct types are supported as default values.
    case class Config(
        dataSource: String,
        useDataFrames: Boolean = false)
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        useDataFrames = true),
      Config(
        "orc"),
      Config(
        "orc",
        useDataFrames = true)).foreach { config =>
      withTable("t") {
        sql(s"create table t(i boolean) using ${config.dataSource}")
        if (config.useDataFrames) {
          Seq((false)).toDF.write.insertInto("t")
        } else {
          sql("insert into t select false")
        }
        sql("alter table t add column s struct<x boolean, y string> default struct(true, 'abc')")
        checkAnswer(spark.table("t"), Row(false, Row(true, "abc")))
      }
    }

    // Negative tests: provided map element types must match their corresponding DEFAULT
    // declarations, if applicable.
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        true)).foreach { config =>
      withTable("t") {
        sql(s"create table t(i boolean) using ${config.dataSource}")
        if (config.useDataFrames) {
          Seq((false)).toDF.write.insertInto("t")
        } else {
          sql("insert into t select false")
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s struct<x boolean, y string> default struct(42, 56)")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "expectedType" -> "\"STRUCT<x: BOOLEAN, y: STRING>\"",
            "defaultValue" -> "struct(42, 56)",
            "actualType" -> "\"STRUCT<col1: INT, col2: INT>\""))
      }
    }
  }

  test("SPARK-39557 INSERT INTO statements with tables with map defaults") {
    // Positive tests: map types are supported as default values.
    case class Config(
        dataSource: String,
        useDataFrames: Boolean = false)
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        useDataFrames = true),
      Config(
        "orc"),
      Config(
        "orc",
        useDataFrames = true)).foreach { config =>
      withTable("t") {
        sql(s"create table t(i boolean) using ${config.dataSource}")
        if (config.useDataFrames) {
          Seq((false)).toDF.write.insertInto("t")
        } else {
          sql("insert into t select false")
        }
        sql("alter table t add column s map<boolean, string> default map(true, 'abc')")
        checkAnswer(spark.table("t"), Row(false, Map(true -> "abc")))
      }
      withTable("t") {
        sql(
          s"""
            create table t(
              i int,
              s struct<
                x array<
                  struct<a int, b int>>,
                y array<
                  map<boolean, string>>>
              default struct(
                array(
                  struct(1, 2)),
                array(
                  map(false, 'def', true, 'jkl'))))
              using ${config.dataSource}""")
        sql("insert into t select 1, default")
        sql("alter table t alter column s drop default")
        if (config.useDataFrames) {
          Seq((2, null)).toDF.write.insertInto("t")
        } else {
          sql("insert into t select 2, default")
        }
        sql(
          """
            alter table t alter column s
            set default struct(
              array(
                struct(3, 4)),
              array(
                map(false, 'mno', true, 'pqr')))""")
        sql("insert into t select 3, default")
        sql(
          """
            alter table t
            add column t array<
              map<boolean, string>>
            default array(
              map(true, 'xyz'))""")
        sql("insert into t(i, s) select 4, default")
        checkAnswer(spark.table("t"),
          Seq(
            Row(1,
              Row(Seq(Row(1, 2)), Seq(Map(false -> "def", true -> "jkl"))),
              Seq(Map(true -> "xyz"))),
            Row(2,
              null,
              Seq(Map(true -> "xyz"))),
            Row(3,
              Row(Seq(Row(3, 4)), Seq(Map(false -> "mno", true -> "pqr"))),
              Seq(Map(true -> "xyz"))),
            Row(4,
              Row(Seq(Row(3, 4)), Seq(Map(false -> "mno", true -> "pqr"))),
              Seq(Map(true -> "xyz")))))
      }
    }
    // Negative tests: provided map element types must match their corresponding DEFAULT
    // declarations, if applicable.
    Seq(
      Config(
        "parquet"),
      Config(
        "parquet",
        true)).foreach { config =>
      withTable("t") {
        sql(s"create table t(i boolean) using ${config.dataSource}")
        if (config.useDataFrames) {
          Seq((false)).toDF.write.insertInto("t")
        } else {
          sql("insert into t select false")
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s map<boolean, string> default map(42, 56)")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "ALTER TABLE ADD COLUMNS",
            "colName" -> "`s`",
            "expectedType" -> "\"MAP<BOOLEAN, STRING>\"",
            "defaultValue" -> "map(42, 56)",
            "actualType" -> "\"MAP<INT, INT>\""))
      }
    }
  }

  test("SPARK-39643 Prohibit subquery expressions in DEFAULT values") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("create table t(a string default (select 'abc')) using parquet")
      },
      errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
      parameters = Map(
        "statement" -> "CREATE TABLE",
        "colName" -> "`a`",
        "defaultValue" -> "(select 'abc')"))
    checkError(
      exception = intercept[AnalysisException] {
        sql("create table t(a string default exists(select 42 where true)) using parquet")
      },
      errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
      parameters = Map(
        "statement" -> "CREATE TABLE",
        "colName" -> "`a`",
        "defaultValue" -> "exists(select 42 where true)"))
    checkError(
      exception = intercept[AnalysisException] {
        sql("create table t(a string default 1 in (select 1 union all select 2)) using parquet")
      },
      errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
      parameters = Map(
        "statement" -> "CREATE TABLE",
        "colName" -> "`a`",
        "defaultValue" -> "1 in (select 1 union all select 2)"))
  }

  test("SPARK-43071: INSERT INTO from queries whose final operators are not projections") {
    def runTest(insert: String, expected: Seq[Row]): Unit = {
      withTable("t1", "t2") {
        sql("create table t1(i boolean, s bigint default 42) using parquet")
        sql("insert into t1 values (true, 41), (false, default)")
        sql("create table t2(i boolean default true, s bigint default 42, " +
          "t string default 'abc') using parquet")
        sql(insert)
        checkAnswer(spark.table("t2"), expected)
      }
    }

    def expectFail(insert: String): Unit = {
      withTable("t1", "t2") {
        sql("create table t1(i boolean, s bigint default 42) using parquet")
        sql("insert into t1 values (true, 41), (false, default)")
        sql("create table t2(i boolean default true, s bigint default 42, " +
          "t string default 'abc') using parquet")
        assert(intercept[AnalysisException](sql(insert)).errorClass.get.startsWith(
          "UNRESOLVED_COLUMN"))
      }
    }
    // The DEFAULT references in these query patterns are detected and replaced.
    runTest("insert into t2 (i, s) select default, s from t1 order by s limit 1",
      Seq(Row(true, 41L, "abc")))
    runTest("insert into t2 (i, s) select default, s from t1 order by s limit 1 offset 1",
      Seq(Row(true, 42L, "abc")))
    runTest("insert into t2 (i, s) select default, default from t1 inner join t1 using (i, s)",
      Seq(Row(true, 42L, "abc"),
        Row(true, 42L, "abc")))
    // The DEFAULT references in these query patterns are not detected.
    expectFail("insert into t2 (i, s) select default, 41L union all select default, 42L")
    expectFail("insert into t2 (i, s) select default, min(s) from t1 group by i")
  }

  test("SPARK-39844 Restrict adding DEFAULT columns for existing tables to certain sources") {
    Seq("csv", "json", "orc", "parquet").foreach { provider =>
      withTable("t1") {
        // Set the allowlist of table providers to include the new table type for all SQL commands.
        withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$provider, ") {
          // It is OK to create a new table with a column DEFAULT value assigned if the table
          // provider is in the allowlist.
          sql(s"create table t1(a int default 42) using $provider")
          // It is OK to add a new column to the table with a DEFAULT value to the existing table
          // since this table provider is not yet present in the
          // 'ADD_DEFAULT_COLUMN_EXISTING_TABLE_BANNED_PROVIDERS' denylist.
          sql(s"alter table t1 add column (b string default 'abc')")
          // Insert a row into the table and check that the assigned DEFAULT value is correct.
          sql(s"insert into t1 values (42, default)")
          checkAnswer(spark.table("t1"), Row(42, "abc"))
        }
        // Now update the allowlist of table providers to prohibit ALTER TABLE ADD COLUMN commands
        // from assigning DEFAULT values.
        withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$provider*") {
          checkV1AndV2Error(
            exception = intercept[AnalysisException] {
              // Try to add another column to the existing table again. This fails because the table
              // provider is now in the denylist.
              sql(s"alter table t1 add column (b string default 'abc')")
            },
            v1ErrorClass = "_LEGACY_ERROR_TEMP_1346",
            v1Parameters = Map(
              "statementType" -> "ALTER TABLE ADD COLUMNS",
              "dataSource" -> provider),
            v2ErrorClass = "FIELDS_ALREADY_EXISTS",
            v2Parameters = Map(
              "op" -> "add",
              "fieldNames" -> "`b`",
              "struct" -> "\"STRUCT<a: INT, b: STRING>\""),
            v2QueryContext = Array(ExpectedContext("", "", 0, 49,
              "alter table t1 add column (b string default 'abc')")))
          withTable("t2") {
            // It is still OK to create a new table with a column DEFAULT value assigned, even if
            // the table provider is in the above denylist.
            sql(s"create table t2(a int default 42) using $provider")
            // Insert a row into the table and check that the assigned DEFAULT value is correct.
            sql(s"insert into t2 values (default)")
            checkAnswer(spark.table("t2"), Row(42))
          }
        }
      }
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withTable("t1") {
      val binaryStr = "Spark SQL"
      val binaryHexStr = Hex.hex(UTF8String.fromString(binaryStr).getBytes).toString
      sql(
        """
          | CREATE TABLE t1(name STRING, part1 DATE, part2 TIMESTAMP, part3 BINARY,
          |  part4 STRING, part5 STRING, part6 STRING, part7 STRING)
          | USING PARQUET PARTITIONED BY (part1, part2, part3, part4, part5, part6, part7)
         """.stripMargin)

      sql(
        s"""
           | INSERT OVERWRITE t1 PARTITION(
           | part1 = date'2019-01-01',
           | part2 = timestamp'2019-01-01 11:11:11',
           | part3 = X'$binaryHexStr',
           | part4 = 'p1',
           | part5 = date'2019-01-01',
           | part6 = timestamp'2019-01-01 11:11:11',
           | part7 = X'$binaryHexStr'
           | ) VALUES('a')
        """.stripMargin)
      checkAnswer(sql(
        """
          | SELECT
          |   name,
          |   CAST(part1 AS STRING),
          |   CAST(part2 as STRING),
          |   CAST(part3 as STRING),
          |   part4,
          |   part5,
          |   part6,
          |   part7
          | FROM t1
        """.stripMargin),
        Row("a", "2019-01-01", "2019-01-01 11:11:11", "Spark SQL", "p1",
          "2019-01-01", "2019-01-01 11:11:11", "Spark SQL"))

      val e = intercept[AnalysisException] {
        sql("CREATE TABLE t2(name STRING, part INTERVAL) USING PARQUET PARTITIONED BY (part)")
      }.getMessage
      assert(e.contains("Cannot use interval"))
    }
  }

  test("SPARK-34556: " +
    "checking duplicate static partition columns should respect case sensitive conf") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      checkError(
        exception = intercept[ParseException] {
          sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
        },
        sqlState = None,
        errorClass = "DUPLICATE_KEY",
        parameters = Map("keyColumn" -> "`c`"),
        context = ExpectedContext("PARTITION (c='2', C='3')", 19, 42)
      )
    }
    // The following code is skipped for Hive because columns stored in Hive Metastore is always
    // case insensitive and we cannot create such table in Hive Metastore.
    if (!format.startsWith("hive")) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        withTable("t") {
          sql(s"CREATE TABLE t(i int, c string, C string) USING PARQUET PARTITIONED BY (c, C)")
          sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
          checkAnswer(spark.table("t"), Row(1, "2", "3"))
        }
      }
    }
  }

  test("SPARK-30844: static partition should also follow StoreAssignmentPolicy") {
    val testingPolicies = if (format == "foo") {
      // DS v2 doesn't support the legacy policy
      Seq(SQLConf.StoreAssignmentPolicy.ANSI, SQLConf.StoreAssignmentPolicy.STRICT)
    } else {
      SQLConf.StoreAssignmentPolicy.values
    }

    def shouldThrowException(policy: SQLConf.StoreAssignmentPolicy.Value): Boolean = policy match {
      case SQLConf.StoreAssignmentPolicy.ANSI | SQLConf.StoreAssignmentPolicy.STRICT =>
        true
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        false
    }

    testingPolicies.foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
        withTable("t") {
          sql("create table t(a int, b string) using parquet partitioned by (a)")
          if (shouldThrowException(policy)) {
            checkError(
              exception = intercept[SparkNumberFormatException] {
                sql("insert into t partition(a='ansi') values('ansi')")
              },
              errorClass = "CAST_INVALID_INPUT",
              parameters = Map(
                "expression" -> "'ansi'",
                "sourceType" -> "\"STRING\"",
                "targetType" -> "\"INT\"",
                "ansiConfig" -> "\"spark.sql.ansi.enabled\""
              ),
              context = ExpectedContext("insert into t partition(a='ansi')", 0, 32)
            )
          } else {
            sql("insert into t partition(a='ansi') values('ansi')")
            checkAnswer(sql("select * from t"), Row("ansi", null) :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-38228: legacy store assignment should not fail on error under ANSI mode") {
    // DS v2 doesn't support the legacy policy
    if (format != "foo") {
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString,
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          withTable("t") {
            sql("create table t(a int) using parquet")
            sql("insert into t values('ansi')")
            checkAnswer(spark.table("t"), Row(null))
          }
        }
      }
    }
  }

  test("SPARK-41982: treat the partition field as string literal " +
    "when keepPartitionSpecAsStringLiteral is enabled") {
    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      withTable("t") {
        sql("create table t(i string, j int) using orc partitioned by (dt string)")
        sql("insert into t partition(dt=08) values('a', 10)")
        Seq(
          "select * from t where dt='08'",
          "select * from t where dt=08"
        ).foreach { query =>
          checkAnswer(sql(query), Seq(Row("a", 10, "08")))
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t drop partition(dt='8')")
          },
          errorClass = "PARTITIONS_NOT_FOUND",
          sqlState = None,
          parameters = Map(
            "partitionList" -> "PARTITION \\(`dt` = 8\\)",
            "tableName" -> ".*`t`"),
          matchPVals = true
        )
      }
    }

    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "false") {
      withTable("t") {
        sql("create table t(i string, j int) using orc partitioned by (dt string)")
        sql("insert into t partition(dt=08) values('a', 10)")
        checkAnswer(sql("select * from t where dt='08'"), sql("select * from t where dt='07'"))
        checkAnswer(sql("select * from t where dt=08"), Seq(Row("a", 10, "8")))
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t drop partition(dt='08')")
          },
          errorClass = "PARTITIONS_NOT_FOUND",
          sqlState = None,
          parameters = Map(
            "partitionList" -> "PARTITION \\(`dt` = 08\\)",
            "tableName" -> ".*.`t`"),
          matchPVals = true
        )
      }
    }
  }
}

class FileSourceSQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {

  override def format: String = "parquet"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String],
      v1QueryContext: Array[QueryContext] = Array.empty,
      v2QueryContext: Array[QueryContext] = Array.empty): Unit = {
    checkError(exception = exception, sqlState = None, errorClass = v1ErrorClass,
      parameters = v1Parameters, queryContext = v1QueryContext)
  }

  override def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Row,
      v2ExpectedAnswer: Row): Unit =
    checkAnswer(df, v1ExpectedAnswer)

  override def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Seq[Row],
      v2ExpectedAnswer: Seq[Row]): Unit =
    checkAnswer(df, v1ExpectedAnswer)

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, format)
  }

}

class DSV2SQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {

  override def format: String = "foo"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String],
      v1QueryContext: Array[QueryContext] = Array.empty,
      v2QueryContext: Array[QueryContext] = Array.empty): Unit = {
    checkError(exception = exception, sqlState = None, errorClass = v2ErrorClass,
      parameters = v2Parameters, queryContext = v2QueryContext)
  }

  override def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Row,
      v2ExpectedAnswer: Row): Unit =
    checkAnswer(df, v2ExpectedAnswer)

  override def checkV1AndV2Answer(
      df: => DataFrame,
      v1ExpectedAnswer: Seq[Row],
      v2ExpectedAnswer: Seq[Row]): Unit =
    checkAnswer(df, v2ExpectedAnswer)

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }

  test("static partition column name should not be used in the column list") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')")
        },
        errorClass = "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
        parameters = Map("staticName" -> "c"))
    }
  }

}
