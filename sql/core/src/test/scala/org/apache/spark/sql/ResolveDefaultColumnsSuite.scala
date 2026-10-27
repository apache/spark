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

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayRepeat, ArrayTransform, AttributeReference, CurrentTime, EqualTo, Expression, Literal, Sha2, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceCurrentLike, SpecialDatetimeValues}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue, Identifier, InMemoryTableCatalog, TableInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}

class ResolveDefaultColumnsSuite extends SharedSparkSession {
  test("column without default value defined (null as default)") {
    withTable("t") {
      sql("create table t(c1 timestamp, c2 timestamp) using parquet")

      // INSERT with user-defined columns
      sql("insert into t (c2) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select null, timestamp'2020-12-31'").collect().head)
      sql("truncate table t")
      sql("insert into t (c1) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-12-31', null").collect().head)

      // INSERT without user-defined columns
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (timestamp'2020-12-31')")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`",
          "dataColumns" -> "`col1`"))
    }
  }

  test("column with default value defined") {
    withTable("t") {
      sql("create table t(c1 timestamp DEFAULT timestamp'2020-01-01', " +
        "c2 timestamp DEFAULT timestamp'2020-01-01') using parquet")

      // INSERT with user-defined columns
      sql("insert into t (c1) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-12-31', timestamp'2020-01-01'").collect().head)
      sql("truncate table t")
      sql("insert into t (c2) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-01-01', timestamp'2020-12-31'").collect().head)

      // INSERT without user-defined columns
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (timestamp'2020-12-31')")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`",
          "dataColumns" -> "`col1`"))
    }
  }

  test("current-time defaults use the enclosing query instant") {
    val instant = Instant.parse("2000-01-01T12:34:56.123456789Z")
    val guardedDivision = "IF(current_date() > DATE'1900-01-01', 1, 1 / 0)"
    val mixedTimeDefault =
      "named_struct('a', CAST(TIME'12:00:00' AS TIMESTAMP_NTZ), 'b', current_timestamp())"
    val mixedTimeType = StructType(Seq(
      StructField("a", TimestampNTZType),
      StructField("b", TimestampType, nullable = false)))
    val defaults = Seq(
      (TimestampType, "current_timestamp()", "current_timestamp()"),
      (TimestampType, "now()", "now()"),
      (TimestampLTZNanosType(9), "current_timestamp(9)", "current_timestamp(9)"),
      (DateType, "current_date()", "current_date()"),
      (TimestampNTZType, "localtimestamp()", "localtimestamp()"),
      (TimestampNTZNanosType(9), "localtimestamp(9)", "localtimestamp(9)"),
      (TimestampType, "current_timestamp() + INTERVAL 1 SECOND",
        "current_timestamp() + INTERVAL 1 SECOND"),
      (StringType, "CAST(current_timestamp() AS STRING)",
        "CAST(current_timestamp() AS STRING)"),
      (StringType, "current_timestamp()", "CAST(current_timestamp() AS STRING)"),
      (ByteType, "day(current_date())", "CAST(day(current_date()) AS TINYINT)"),
      (DoubleType, guardedDivision, guardedDivision),
      (mixedTimeType, mixedTimeDefault, mixedTimeDefault))

    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      defaults.foreach { case (dataType, defaultSQL, referenceSQL) =>
        val field = StructField("value", dataType).withCurrentDefaultValue(defaultSQL)
        val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
        val reference = sql(s"SELECT $referenceSQL").queryExecution.analyzed
          .asInstanceOf[Project].projectList.head
        val comparison = EqualTo(default, reference)
        withClue(s"$defaultSQL as $dataType: ") {
          assert(ComputeCurrentTime.applyForExpression(comparison, instant).eval() == true)
        }
      }
    }
  }

  test("current-time defaults match explicit timestamps in INSERT") {
    withTable("t") {
      sql("CREATE TABLE t(i INT, d1 TIMESTAMP DEFAULT current_timestamp(), " +
        "d2 TIMESTAMP DEFAULT current_timestamp(), explicit_ts TIMESTAMP) USING parquet")
      sql("INSERT INTO t VALUES (1, DEFAULT, DEFAULT, current_timestamp()), " +
        "(2, DEFAULT, DEFAULT, current_timestamp())")
      sql("INSERT INTO t (i, explicit_ts) VALUES " +
        "(3, current_timestamp()), (4, current_timestamp())")
      sql("INSERT INTO t SELECT CAST(id AS INT), DEFAULT, DEFAULT, current_timestamp() " +
        "FROM range(5, 7)")
      sql("INSERT INTO t (i, explicit_ts) SELECT CAST(id AS INT), current_timestamp() " +
        "FROM range(7, 9)")

      checkAnswer(
        sql("SELECT i, d1 = d2 AND d1 = explicit_ts FROM t"),
        (1 to 8).map(i => Row(i, true)))
    }
  }

  gridTest("current-time defaults materialize omitted VALUES defaults once")(Seq(1, 4)) {
    rowCount =>
      val defaultSQL = "sha2(repeat(CAST(current_timestamp() AS STRING), 1000), 256)"
      val tableName = "local_values_defaults"
      // The shared session disables local conversion to preserve execution coverage.
      withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withTable(tableName) {
          sql(s"CREATE TABLE $tableName(col1 INT, h STRING DEFAULT ($defaultSQL), " +
            "col2 TIMESTAMP) USING parquet")

          val values = (1 to rowCount).map(i => s"($i, current_timestamp())").mkString(", ")
          val parsed = spark.sessionState.sqlParser.parsePlan(
            s"INSERT INTO $tableName BY NAME VALUES $values")
          val analyzed = spark.sessionState.analyzer.execute(parsed)
          spark.sessionState.analyzer.checkAnalysis(analyzed)
          val counter = new AtomicInteger()
          case class CountedLocalDefault(child: Expression)
              extends UnaryExpression with CodegenFallback {
            override def dataType: DataType = child.dataType
            override def eval(input: InternalRow): Any = {
              counter.incrementAndGet()
              child.eval(input)
            }
            override protected def withNewChildInternal(
                newChild: Expression): CountedLocalDefault = copy(child = newChild)
          }
          var instrumented = 0
          val counted = analyzed.transformUp {
            case project: Project =>
              project.copy(projectList = project.projectList.map {
                case alias: Alias if alias.name == "h" &&
                    alias.child.exists(_.isInstanceOf[Sha2]) =>
                  instrumented += 1
                  alias.withNewChildren(Seq(CountedLocalDefault(alias.child))).asInstanceOf[Alias]
                case other => other
              })
          }
          assert(instrumented == 1, analyzed.treeString)
          counter.set(0)
          classic.Dataset.ofRows(spark, counted).collect()

          checkAnswer(
            sql(s"SELECT col1, h = sha2(repeat(CAST(col2 AS STRING), 1000), 256) " +
              s"FROM $tableName"),
            (1 to rowCount).map(i => Row(i, true)))
          assert(counter.get() == 1)
        }
      }
  }

  namedGridTest("current-time defaults preserve short-circuit guards")(Map(
    "true-left OR" -> (BooleanType,
      "current_date() IS NOT NULL OR 1 / 0 > 0"),
    "false-left AND" -> (BooleanType,
      "current_date() IS NULL AND 1 / 0 > 0"),
    "null-left comparison" -> (BooleanType,
      "IF(current_date() IS NOT NULL, CAST(NULL AS INT), 1) = 1 / 0"),
    "special datetime cast" -> (TimestampType,
      "CAST(IF(current_date() IS NOT NULL, 'epoch', " +
        "CAST(CAST(CAST(1 / 0 AS STRING) AS TIMESTAMP) AS STRING)) AS TIMESTAMP)"))) {
    case (dataType, defaultSQL) =>
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val field = StructField("value", dataType).withCurrentDefaultValue(defaultSQL)
        assert(ResolveDefaultColumns.analyze(field, "INSERT").dataType == dataType)
        val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
        val reference = sql(s"SELECT $defaultSQL").queryExecution.analyzed
          .asInstanceOf[Project].projectList.head
        val instant = Instant.parse("2000-01-01T12:34:56.123456Z")
        val actual = SpecialDatetimeValues.applyForExpression(
          ComputeCurrentTime.applyForExpression(default, instant)).eval()
        val expected = SpecialDatetimeValues.applyForExpression(
          ComputeCurrentTime.applyForExpression(reference, instant)).eval()
        assert(actual == expected)
      }
  }

  test("current-time defaults reject evaluated invalid operands") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val field = StructField("value", BooleanType)
        .withCurrentDefaultValue("current_date() IS NULL OR 1 / 0 > 0")
      val resolvers = Seq(
        () => ResolveDefaultColumns.analyze(field, "INSERT"),
        () => ResolveDefaultColumns.getDefaultValueExprOrNullLit(field))
      resolvers.foreach { resolve =>
        assert(intercept[SparkArithmeticException](resolve()).getCondition == "DIVIDE_BY_ZERO")
      }
    }
  }

  test("current-time defaults retain unary clocks until snapshot binding") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      val field = StructField("value", BooleanType)
        .withCurrentDefaultValue("current_time(9) = current_time(9)")
      val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
      assert(default.collect { case _: CurrentTime => 1 }.size == 2)
      val instant = Instant.parse("2000-01-01T12:34:56.123456789Z")
      assert(ComputeCurrentTime.applyForExpression(default, instant).eval() == true)
    }
  }

  test("current-time defaults compare repeated current_time calls in INSERT") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      withTable("t") {
        sql("CREATE TABLE t(i INT, b BOOLEAN DEFAULT " +
          "(current_time(9) = current_time(9))) USING parquet")
        sql("INSERT INTO t VALUES (1, DEFAULT), (2, DEFAULT)")
        sql("INSERT INTO t (i) VALUES (3), (4)")
        checkAnswer(sql("SELECT * FROM t"), (1 to 4).map(i => Row(i, true)))
      }
    }
  }

  test("current-time defaults support clock-dependent struct field names") {
    val dataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))
    val defaultSQL = "named_struct(IF(current_date() IS NOT NULL, 'a', 'a'), 1)"
    val field = StructField("s", dataType).withCurrentDefaultValue(defaultSQL)
    withSQLConf(
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
        "spark.sql.catalog.current_time_defaults" -> classOf[InMemoryTableCatalog].getName) {
      val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
      assert(default.dataType == dataType)
      val instant = Instant.parse("2000-01-01T12:34:56.123456Z")
      val value = ComputeCurrentTime.applyForExpression(default, instant)
        .eval().asInstanceOf[InternalRow]
      assert(value.getInt(0) == 1)

      val tableName = "current_time_defaults.named_struct"
      withTable(tableName) {
        val catalog = spark.sessionState.catalogManager.catalog("current_time_defaults")
          .asInstanceOf[InMemoryTableCatalog]
        val columns = Array(
          Column.create("i", IntegerType),
          Column.create("s", dataType, true, null,
            new ColumnDefaultValue(defaultSQL, null), "{}"))
        catalog.createTable(Identifier.of(Array.empty, "named_struct"),
          new TableInfo.Builder().withColumns(columns).build())
        sql(s"INSERT INTO $tableName SELECT 1, DEFAULT")
        sql(s"INSERT INTO $tableName (i) SELECT 2")
        checkAnswer(spark.table(tableName), Seq(Row(1, Row(1)), Row(2, Row(1))))
      }
    }
  }

  test("current-time defaults materialize checked constant arrays") {
    val dataType = ArrayType(TimestampType, containsNull = false)
    val defaultSQL = "array_repeat(CAST(IF(current_date() > DATE'1900-01-01', " +
      "'epoch', CAST(NULL AS STRING)) AS TIMESTAMP), 1000)"
    withSQLConf(
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        "spark.sql.catalog.current_time_defaults" -> classOf[InMemoryTableCatalog].getName) {
      val tableName = "current_time_defaults.arrays"
      withTable(tableName) {
        val catalog = spark.sessionState.catalogManager.catalog("current_time_defaults")
          .asInstanceOf[InMemoryTableCatalog]
        val columns = Array(
          Column.create("i", LongType),
          Column.create("a", dataType, true, null,
            new ColumnDefaultValue(defaultSQL, null), "{}"))
        catalog.createTable(Identifier.of(Array.empty, "arrays"),
          new TableInfo.Builder().withColumns(columns).build())
        sql(s"INSERT INTO $tableName SELECT id, DEFAULT FROM range(3)")
        sql(s"INSERT INTO $tableName (i) SELECT id FROM range(3, 6)")
        assert(spark.table(tableName).schema("a").dataType == dataType)
        checkAnswer(
          sql(s"SELECT i, size(a), array_distinct(a) = " +
            s"array(TIMESTAMP'1970-01-01 00:00:00') FROM $tableName"),
          (0L until 6L).map(i => Row(i, 1000, true)))
      }

      val field = StructField("a", dataType).withCurrentDefaultValue(defaultSQL)
      val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
      val projection = Project(Seq(Alias(default, "a")()), spark.range(6).queryExecution.analyzed)
      spark.sessionState.analyzer.checkAnalysis(projection)
      val optimized = spark.sessionState.optimizer.execute(projection).asInstanceOf[Project]
      val materialized = optimized.projectList.head.asInstanceOf[Alias].child
      assert(materialized.isInstanceOf[Literal], optimized.treeString)
      assert(!materialized.exists(_.isInstanceOf[ArrayTransform]))
      assert(materialized.dataType == dataType)
    }

    for {
      ansiEnabled <- Seq(true, false)
      storeAssignmentPolicy <- Seq("ANSI", "LEGACY")
    } {
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString,
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val field = StructField("a", dataType).withCurrentDefaultValue(defaultSQL)
        val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
        val past = Instant.parse("1800-01-01T12:34:56.123456Z")
        val nullDefault = SpecialDatetimeValues.applyForExpression(
          ComputeCurrentTime.applyForExpression(default, past))
        assert(intercept[SparkRuntimeException](nullDefault.eval()).getCondition ==
          "NOT_NULL_ASSERT_VIOLATION")
      }
    }
  }

  gridTest("current-time defaults construct checked structs once")(
      for (width <- Seq(2, 16); arraySize <- Seq(8, 1000)) yield (width, arraySize)) {
    case (width, arraySize) =>
      withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val dataType = StructType(Seq(
          StructField("required", TimestampType, nullable = false),
          StructField("payload", ArrayType(TimestampType, containsNull = false),
            nullable = false)) ++
          (2 until width).map(i => StructField(s"f$i", IntegerType, nullable = false)))
        val fields = Seq(
          "'required', CAST(IF(current_date() > DATE'1900-01-01', " +
            "'epoch', CAST(NULL AS STRING)) AS TIMESTAMP)",
          s"'payload', array_repeat(current_timestamp(), $arraySize)") ++
          (2 until width).map(i => s"'f$i', $i")
        val defaultSQL = s"named_struct(${fields.mkString(", ")})"
        val field = StructField("s", dataType).withCurrentDefaultValue(defaultSQL)
        val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
        val counter = new AtomicInteger()
        case class CountedDefault(child: Expression)
            extends UnaryExpression with CodegenFallback {
          override def nullable: Boolean = child.nullable
          override def dataType: DataType = child.dataType
          override def eval(input: InternalRow): Any = {
            counter.incrementAndGet()
            child.eval(input)
          }
          override protected def withNewChildInternal(newChild: Expression): CountedDefault =
            copy(child = newChild)
        }
        val counted = default.transformUp {
          case array: ArrayRepeat => CountedDefault(array)
        }
        val instant = Instant.parse("2000-01-01T12:34:56.123456Z")
        val timedDefault = SpecialDatetimeValues.applyForExpression(
          ComputeCurrentTime.applyForExpression(counted, instant))
        counter.set(0)
        val value = timedDefault.eval().asInstanceOf[InternalRow]
        assert(counter.get() == 1)
        assert(value.getLong(0) == 0L)
        assert(value.getArray(1).numElements() == arraySize)
        assert(timedDefault.dataType == dataType)
      }
  }

  test("current-time defaults preserve nested NOT NULL fields") {
    val instant = Instant.parse("2000-01-01T12:34:56.123456Z")
    val dataType = StructType(Seq(
      StructField("a", TimestampType, nullable = false),
      StructField("b", TimestampType)))
    val defaults = Seq(
      "named_struct('a', CAST('epoch' AS TIMESTAMP), 'b', current_timestamp())",
      "named_struct('a', CAST(IF(current_date() > DATE'1900-01-01', " +
        "'epoch', CAST(NULL AS STRING)) AS TIMESTAMP), 'b', current_timestamp())")
    val invalidDefaultSQL =
      "named_struct('a', IF(true, TIMESTAMP'1970-01-01', CAST(NULL AS TIMESTAMP)), " +
        "'b', current_timestamp())"
    val invalidField = StructField("s", dataType).withCurrentDefaultValue(invalidDefaultSQL)
    val tableName = "current_time_defaults.t"

    for {
      ansiEnabled <- Seq(true, false)
      storeAssignmentPolicy <- Seq("ANSI", "LEGACY")
      defaultSQL <- defaults
    } {
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString,
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.catalog.current_time_defaults" -> classOf[InMemoryTableCatalog].getName) {
        val invalidResolvers = Seq(
          () => ResolveDefaultColumns.analyze(invalidField, "INSERT"),
          () => ResolveDefaultColumns.getDefaultValueExprOrNullLit(invalidField))
        invalidResolvers.foreach { resolve =>
          assert(intercept[AnalysisException](resolve()).getCondition ==
            "INVALID_DEFAULT_VALUE.DATA_TYPE")
        }
        val field = StructField("s", dataType).withCurrentDefaultValue(defaultSQL)
        assert(ResolveDefaultColumns.analyze(field, "INSERT").dataType == dataType)
        val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
        val projection = Project(Seq(Alias(default, "s")()), OneRowRelation())
        spark.sessionState.analyzer.checkAnalysis(projection)
        assert(default.dataType == dataType)
        val timedDefault = ComputeCurrentTime.applyForExpression(default, instant)
        val value = SpecialDatetimeValues.applyForExpression(timedDefault)
          .eval().asInstanceOf[InternalRow]
        assert(value.getLong(0) == 0L)
        assert(value.getLong(1) == DateTimeUtils.instantToMicros(instant))

        if (defaultSQL == defaults.last) {
          val past = Instant.parse("1800-01-01T12:34:56.123456Z")
          val nullDefault = SpecialDatetimeValues.applyForExpression(
            ComputeCurrentTime.applyForExpression(default, past))
          assert(intercept[SparkRuntimeException](nullDefault.eval()).getCondition ==
            "NOT_NULL_ASSERT_VIOLATION")
        }

        if (storeAssignmentPolicy == "ANSI") {
          withTable(tableName) {
            val catalog = spark.sessionState.catalogManager.catalog("current_time_defaults")
              .asInstanceOf[InMemoryTableCatalog]
            val columns = Array(
              Column.create("i", IntegerType),
              Column.create("s", dataType, true, null,
                new ColumnDefaultValue(defaultSQL, null), "{}"),
              Column.create("explicit_ts", TimestampType))
            catalog.createTable(Identifier.of(Array.empty, "t"),
              new TableInfo.Builder().withColumns(columns).build())
            sql(s"INSERT INTO $tableName VALUES (1, DEFAULT, current_timestamp()), " +
              "(2, DEFAULT, current_timestamp())")
            sql(s"INSERT INTO $tableName (i, explicit_ts) VALUES " +
              "(3, current_timestamp()), (4, current_timestamp())")
            sql(s"INSERT INTO $tableName " +
              "SELECT CAST(id AS INT), DEFAULT, current_timestamp() FROM range(5, 7)")
            sql(s"INSERT INTO $tableName (i, explicit_ts) " +
              "SELECT CAST(id AS INT), current_timestamp() FROM range(7, 9)")
            checkAnswer(
              sql(s"SELECT i, s.a = TIMESTAMP'1970-01-01 00:00:00', " +
                s"s.b = explicit_ts FROM $tableName"),
              (1 to 8).map(i => Row(i, true, true)))
          }
        }
      }
    }
  }

  test("current-time defaults retain their default analyzer namespace") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withCurrentCatalogAndNamespace {
        withDatabase("current_time_namespace") {
          sql("CREATE DATABASE current_time_namespace")
          sql("USE current_time_namespace")
          val namespace = ResolveDefaultColumns
            .analyze("namespace", StringType, "current_database()", "INSERT").eval().toString
          assert(namespace != spark.catalog.currentDatabase)
          val defaultSQL =
            "concat(current_database(), ':', CAST(current_timestamp() AS STRING))"
          val field = StructField("value", StringType).withCurrentDefaultValue(defaultSQL)
          val default = ResolveDefaultColumns.getDefaultValueExprOrNullLit(field)
          val bound = ReplaceCurrentLike(spark.sessionState.catalogManager)
            .applyForExpression(default)
          val instant = Instant.parse("2000-01-01T12:34:56.123456Z")
          val value = ComputeCurrentTime.applyForExpression(bound, instant).eval().toString
          assert(value == s"$namespace:2000-01-01 12:34:56.123456")
        }
      }
    }
  }

  test("INSERT into partitioned tables") {
    withTable("t") {
      sql("create table t(c1 int, c2 int, c3 int, c4 int) using parquet partitioned by (c3, c4)")

      // INSERT without static partitions
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (1, 2, 3)")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`, `col2`, `col3`"))

      // INSERT without static partitions but with column list
      sql("truncate table t")
      sql("insert into t (c2, c1, c4) values (1, 2, 3)")
      checkAnswer(spark.table("t"), Row(2, 1, null, 3))

      // INSERT with static partitions
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t partition(c3=3, c4=4) values (1)")
        },
        condition = "INSERT_PARTITION_COLUMN_ARITY_MISMATCH",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`",
          "staticPartCols" -> "`c3`, `c4`"))

      // INSERT with static partitions and with column list
      sql("truncate table t")
      sql("insert into t partition(c3=3, c4=4) (c2) values (1)")
      checkAnswer(spark.table("t"), Row(null, 1, 3, 4))

      // INSERT with partial static partitions
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t partition(c3=3, c4) values (1, 2)")
        },
        condition = "INSERT_PARTITION_COLUMN_ARITY_MISMATCH",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`, `col2`",
          "staticPartCols" -> "`c3`"))

      // INSERT with partial static partitions and with column list is not allowed
      intercept[AnalysisException](sql("insert into t partition(c3=3, c4) (c1) values (1, 4)"))
    }
  }

  test("SPARK-43085: Column DEFAULT assignment for target tables with multi-part names") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("insert into demos.test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("use database demos")
        sql("insert into test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
    }
  }

  test("SPARK-43313: Column default values with implicit coercion from provided values") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        // If the provided default value is a literal of a wider type than the target column, but
        // the literal value fits within the narrower type, just coerce it for convenience.
        sql(
          """create table demos.test_ts (
            |a int default 42L,
            |b timestamp_ntz default '2022-01-02',
            |c date default '2022-01-03',
            |f float default 0D
            |) using parquet""".stripMargin)
        sql("insert into demos.test_ts(a) values (default)")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select 42, timestamp_ntz'2022-01-02', date'2022-01-03', 0f"))
        // If the provided default value is a literal of a different type than the target column
        // such that no coercion is possible, throw an error.
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default 'abc') using parquet")
          },
          condition = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "'abc'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a timestamp default 'invalid') using parquet")
          },
          condition = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"TIMESTAMP\"",
            "defaultValue" -> "'invalid'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a boolean default 'true') using parquet")
          },
          condition = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"BOOLEAN\"",
            "defaultValue" -> "'true'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default true) using parquet")
          },
          condition = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "true",
            "actualType" -> "\"BOOLEAN\""))
      }
    }
  }

  test("SPARK-46949: DDL with valid default char/varchar values") {
    withTable("t") {
      val ddl =
        s"""
           |CREATE TABLE t(
           |  key int,
           |  v VARCHAR(6) DEFAULT 'apache',
           |  c CHAR(5) DEFAULT 'spark')
           |USING parquet""".stripMargin
      sql(ddl)
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select * from t"), Row(1, "apache", "spark"))
    }
  }

  test("SPARK-46949: DDL with invalid default char/varchar values") {
    Seq("CHAR", "VARCHAR").foreach { typeName =>
      checkError(
        exception = intercept[SparkRuntimeException](
          sql(s"CREATE TABLE t(c $typeName(3) DEFAULT 'spark') USING parquet")),
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))
    }
  }

  test("SPARK-46949: DDL with default char/varchar values need padding") {
    withTable("t") {
      val ddl =
        s"""
           |CREATE TABLE t(
           |  key int,
           |  v VARCHAR(6) DEFAULT 'apache',
           |  c CHAR(6) DEFAULT 'spark')
           |USING parquet""".stripMargin
      sql(ddl)
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select * from t"), Row(1, "apache", "spark "))
    }
  }

  test("SPARK-46958: timestamp should have timezone for resolvable if default values castable") {
    val defaults = Seq("timestamp '2018-11-17'", "CAST(timestamp '2018-11-17' AS STRING)")
    defaults.foreach { default =>
      withTable("t") {
        sql(s"CREATE TABLE t(key int, c STRING DEFAULT $default) " +
          s"USING parquet")
        sql("INSERT INTO t (key) VALUES(1)")
        checkAnswer(sql("select * from t"), Row(1, "2018-11-17 00:00:00"))
      }
    }
  }

  test("SPARK-46958: timestamp should have timezone for resolvable when default values fit") {
    withTable("t") {
      // If the provided default value is a literal of a wider type than the target column, but
      // the literal value fits within the narrower type, just coerce it for convenience.
      sql(s"CREATE TABLE t(key int, c timestamp DEFAULT '2018-11-17 13:33:33') " +
        s"USING parquet")
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select CAST(c as STRING) from t"), Row("2018-11-17 13:33:33"))
    }
  }

  test("SPARK-48033: default columns using runtime replaceable expression works") {
    withTable("t") {
      sql("CREATE TABLE t(v VARIANT DEFAULT parse_json('1')) USING PARQUET")
      sql("INSERT INTO t VALUES(DEFAULT)")
      checkAnswer(sql("select v from t"), sql("select parse_json('1')").collect())
    }
  }

  test("SPARK-49054: Create table with current_user() default") {
    val tableName = "test_current_user"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s string default current_user()) USING parquet")
      sql(s"INSERT INTO $tableName (i) VALUES ((0))")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(0, user)))
    }
  }

  test("SPARK-49054: Alter table with current_user() default") {
    val tableName = "test_current_user"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s string) USING parquet")
      sql(s"ALTER TABLE $tableName ALTER COLUMN s SET DEFAULT current_user()")
      sql(s"INSERT INTO $tableName (i) VALUES ((0))")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(0, user)))
    }
  }

  test("SPARK-57187: current_user() as default for CHAR column should not throw INTERNAL_ERROR") {
    val tableName = "test_current_user_char"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s CHAR(100) DEFAULT current_user()) USING parquet")
      sql(s"INSERT INTO $tableName (i) VALUES (1)")
      val result = sql(s"SELECT i, TRIM(s) FROM $tableName").collect()
      assert(result.length == 1)
      assert(result.head.getInt(0) == 1)
      assert(result.head.getString(1) == user)
    }
  }

  test("SPARK-57187: current_user() as default for VARCHAR column") {
    val tableName = "test_current_user_varchar"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s VARCHAR(100) DEFAULT current_user()) USING parquet")
      sql(s"INSERT INTO $tableName (i) VALUES (1)")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, user)))
    }
  }

  test("SPARK-57187: ALTER TABLE with current_user() default for CHAR column") {
    val tableName = "test_current_user_char_alter"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(id INT, created_by CHAR(100)) USING parquet")
      sql(s"ALTER TABLE $tableName ALTER COLUMN created_by SET DEFAULT current_user()")
      sql(s"INSERT INTO $tableName (id) VALUES (1)")
      val result = sql(s"SELECT id, TRIM(created_by) FROM $tableName").collect()
      assert(result.length == 1)
      assert(result.head.getInt(0) == 1)
      assert(result.head.getString(1) == user)
    }
  }

  test("SPARK-57187: foldable default exceeding CHAR/VARCHAR length fails at DDL time") {
    // Foldable expressions are still validated eagerly at DDL time (existing behavior)
    Seq("CHAR", "VARCHAR").foreach { typeName =>
      checkError(
        exception = intercept[SparkRuntimeException](
          sql(s"CREATE TABLE t(c $typeName(3) DEFAULT 'toolong') USING parquet")),
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))
    }
  }

  test("SPARK-57187: non-foldable default exceeding CHAR/VARCHAR length fails at INSERT time " +
      "(implicit default)") {
    // current_user() exceeds CHAR(1)/VARCHAR(1) -- DDL succeeds because the expression is
    // non-foldable, but INSERT should fail at runtime with EXCEED_LIMIT_LENGTH.
    Seq("CHAR", "VARCHAR").foreach { typeName =>
      withTable("t") {
        sql(s"CREATE TABLE t(i INT, s $typeName(1) DEFAULT current_user()) USING parquet")
        checkError(
          exception = intercept[SparkRuntimeException](
            sql("INSERT INTO t (i) VALUES (1)")),
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "1"))
      }
    }
  }

  test("SPARK-57187: non-foldable default exceeding CHAR/VARCHAR length fails at INSERT time " +
      "(explicit DEFAULT keyword)") {
    // Using the explicit DEFAULT keyword in VALUES goes through the checkField path.
    Seq("CHAR", "VARCHAR").foreach { typeName =>
      withTable("t") {
        sql(s"CREATE TABLE t(i INT, s $typeName(1) DEFAULT current_user()) USING parquet")
        checkError(
          exception = intercept[SparkRuntimeException](
            sql("INSERT INTO t VALUES (1, DEFAULT)")),
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "1"))
      }
    }
  }

  test("SPARK-57187: by-position default fill applies the CHAR/VARCHAR length check") {
    // The by-position fill path (resolveColumnsByPosition under RECURSE / V2 schema evolution)
    // shares the same applyDefaultWithLengthCheck helper as the by-name path. This drives that
    // path directly and asserts the trailing default column is wrapped with the write-side
    // length check, so an oversized non-foldable default is caught at runtime there too.
    // Expected schema: (i INT, s CHAR(100) DEFAULT current_user()). CHAR is stored as StringType
    // plus the raw-type metadata, exactly as the catalog represents it. CHAR(100) is wide enough
    // that the resolved default does not trip the eager DDL-time length check, so we observe the
    // write-side runtime check that the by-position fill path now adds.
    val charMeta = new MetadataBuilder()
      .putString(CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, "char(100)")
      .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "current_user()")
      .build()
    val expected = Seq(
      AttributeReference("i", IntegerType)(),
      AttributeReference("s", StringType, nullable = true, metadata = charMeta)())
    // A by-position INSERT that supplies only the leading column, omitting the trailing CHAR one.
    val query = LocalRelation(AttributeReference("i", IntegerType)())

    val resolved = TableOutputResolver.resolveOutputColumns(
      "t", expected, query, byName = false, spark.sessionState.conf,
      TableOutputResolver.DefaultValueFillMode.RECURSE)

    val hasLengthCheck = resolved.expressions.exists(_.exists {
      case s: StaticInvoke => s.functionName == "charTypeWriteSideCheck"
      case _ => false
    })
    assert(hasLengthCheck,
      "by-position default fill must apply the CHAR/VARCHAR write-side length check")
  }
}
