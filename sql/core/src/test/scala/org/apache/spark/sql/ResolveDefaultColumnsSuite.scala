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

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Try

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Between, Divide, Expression, LeafExpression, Literal, NullIf, Rand, TypeOf}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{InMemoryRowLevelOperationTableCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, MetadataBuilder, StringType, StructField}

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

  test("SPARK-60029: a default using NULLIF or BETWEEN is constant") {
    // NULLIF and BETWEEN build a `With`, which is not foldable. The conf pins that form.
    withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false",
        "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq("t" -> "parquet", "testcat.ns.t" -> "foo").foreach { case (t, provider) =>
        withTable(t) {
          sql(s"CREATE TABLE $t (id INT, a INT DEFAULT nullif(1, 3), " +
            s"b BOOLEAN DEFAULT (1 + 1) BETWEEN 1 AND nullif(3, 4)) USING $provider")
          sql(s"INSERT INTO $t (id) VALUES (1)")
          sql(s"ALTER TABLE $t ADD COLUMN c INT DEFAULT nullif(nullif(abs(-2), 3), 4)")
          sql(s"ALTER TABLE $t ALTER COLUMN a SET DEFAULT nullif(2, 3)")
          sql(s"INSERT INTO $t (id) VALUES (2)")
          sql(s"INSERT INTO $t VALUES (3, DEFAULT, DEFAULT, DEFAULT)")
          checkAnswer(spark.table(t),
            Seq(Row(1, 1, true, 2), Row(2, 2, true, 2), Row(3, 2, true, 2)))
        }
      }
      // A connector may store an existence default that is not folded yet.
      val field = StructField("c", IntegerType, metadata = new MetadataBuilder()
        .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "nullif(1, 3)")
        .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "nullif(1, 3)")
        .build())
      assert(ResolveDefaultColumns.getExistenceDefaultValue(field) == 1)
    }
  }

  test("SPARK-60029: UPDATE and MERGE set a default using NULLIF or BETWEEN") {
    // The conf pins the `With` form of NULLIF and BETWEEN.
    withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false",
        "spark.sql.catalog.cat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      withTable("cat.ns.t") {
        sql("CREATE TABLE cat.ns.t (id INT, a INT DEFAULT nullif(1, 3), " +
          "b BOOLEAN DEFAULT (1 + 1) BETWEEN 1 AND 3) USING foo")
        sql("INSERT INTO cat.ns.t VALUES (1, 5, false), (2, 5, false)")
        sql("UPDATE cat.ns.t SET a = DEFAULT, b = DEFAULT WHERE id = 1")
        sql("MERGE INTO cat.ns.t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET a = DEFAULT, b = DEFAULT")
        checkAnswer(spark.table("cat.ns.t"), Seq(Row(1, 1, true), Row(2, 1, true)))
      }
    }
  }

  /**
   * Analyzes, as a default read from column metadata, the expression that `build` makes over a
   * foldable integer counting its evaluations, and returns the result and the count.
   */
  private def analyzeCounting(dataType: DataType)(
      build: Expression => Expression): (Try[Expression], Int) = {
    val evaluations = new AtomicInteger()
    val field = StructField("c", dataType, metadata = new MetadataBuilder()
      .putExpression(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "default",
        Some(build(CountingIntLiteral(2, evaluations))))
      .build())
    (Try(ResolveDefaultColumns.analyze(field, "INSERT")), evaluations.get)
  }

  test("SPARK-60029: a default using NULLIF or BETWEEN evaluates its input at most once") {
    // The conf pins the `With` form of NULLIF and BETWEEN, which they build when it is false.
    withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false") {
      // Each NULLIF reads the level below twice, in the comparison and as the result, and so does
      // BETWEEN. Copying the definitions instead would evaluate the counted leaf 2^11 times.
      val (nested, nestedCount) = analyzeCounting(BooleanType) { leaf =>
        val nullIfs = (3 to 12).foldLeft(leaf)((e, i) => new NullIf(e, Literal(i)))
        Between(nullIfs, Literal(1), Literal(3))
      }
      assert(nested.get == Literal(true))
      assert(nestedCount == 1)
      // A default whose comparison reads `rand()` or fails to evaluate is rejected, and the leaf
      // is evaluated once on the way.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        Seq[Expression => Expression](
          leaf => new NullIf(leaf, Rand(Literal(0L))),
          leaf => new NullIf(leaf, Divide(Literal(1), Literal(0)))).foreach { build =>
          val (rejected, rejectedCount) = analyzeCounting(IntegerType)(build)
          checkError(
            exception = intercept[AnalysisException](rejected.get),
            condition = "INVALID_DEFAULT_VALUE.NOT_CONSTANT",
            parameters = Map("statement" -> "INSERT", "colName" -> "`c`",
              "defaultValue" -> "default"))
          assert(rejectedCount == 1)
        }
      }
      // `typeof` reads only the type of its argument, so the leaf is not evaluated.
      val (typeName, typeNameCount) =
        analyzeCounting(StringType)(leaf => TypeOf(new NullIf(leaf, Literal(3))))
      assert(typeName.get == Literal("int"))
      assert(typeNameCount == 0)
    }
  }

  test("SPARK-60029: a default using NULLIF that does not fold is rejected") {
    // The conf pins the `With` form of NULLIF, which leaves `1 / 0` in a conditional branch.
    withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false",
        "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      // DDL rejects a nondeterministic default before it is folded.
      Seq("t" -> "parquet", "testcat.ns.t" -> "foo").foreach { case (t, provider) =>
        withTable(t) {
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"CREATE TABLE $t (id INT, c DOUBLE DEFAULT nullif(rand(), 0.5)) " +
                s"USING $provider")
            },
            condition = "INVALID_DEFAULT_VALUE.NON_DETERMINISTIC",
            parameters = Map("statement" -> "CREATE TABLE", "colName" -> "`c`",
              "defaultValue" -> "nullif(rand(), 0.5)"))
        }
      }
      // A default read from a catalog is checked only by `analyze`.
      val field = StructField("c", DoubleType, metadata = new MetadataBuilder()
        .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
          "nullif(rand(), 0.5)")
        .build())
      checkError(
        exception = intercept[AnalysisException](ResolveDefaultColumns.analyze(field, "INSERT")),
        condition = "INVALID_DEFAULT_VALUE.NOT_CONSTANT",
        parameters = Map("statement" -> "INSERT", "colName" -> "`c`",
          "defaultValue" -> "nullif(rand(), 0.5)"))
      // A `With` that fails to fold is left in place, so the default is rejected as before.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withTable("t") {
          checkError(
            exception = intercept[AnalysisException] {
              sql("CREATE TABLE t (id INT, c INT DEFAULT nullif(1, 1 / 0)) USING parquet")
            },
            condition = "INVALID_DEFAULT_VALUE.NOT_CONSTANT",
            parameters = Map("statement" -> "CREATE TABLE", "colName" -> "`c`",
              "defaultValue" -> "nullif(1, 1 / 0)"))
        }
      }
    }
  }
}

/** A foldable integer that counts how many times it is evaluated. */
private case class CountingIntLiteral(value: Int, evaluations: AtomicInteger)
  extends LeafExpression with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override def foldable: Boolean = true

  override def eval(input: InternalRow): Any = {
    evaluations.incrementAndGet()
    value
  }
}
