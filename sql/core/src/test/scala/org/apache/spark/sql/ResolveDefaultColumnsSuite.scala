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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, ResolveDefaultColumns}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructType}

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

  for {
    format <- Seq("parquet", "orc", "json")
    padding <- Seq(None, Some("false"), Some("true"))
  } {
    test(s"CHAR existence defaults use write-side values: $format, padding=$padding") {
      val conf = padding.map(SQLConf.READ_SIDE_CHAR_PADDING.key -> _).toSeq
      withSQLConf(conf: _*) {
        assert(spark.sessionState.conf.readSideCharPadding == padding.getOrElse("true").toBoolean)
        withTable("t") {
          sql(s"CREATE TABLE t(id INT) USING $format")
          sql("INSERT INTO t VALUES (1)")
          sql("ALTER TABLE t ADD COLUMNS (" +
            "region CHAR(3) DEFAULT 'EU', label VARCHAR(3) DEFAULT 'US', " +
            "empty CHAR(3) DEFAULT '', missing CHAR(3) DEFAULT NULL, " +
            "trimmed CHAR(3) DEFAULT 'EU  ', trimmed_v VARCHAR(3) DEFAULT 'US  ')")
          sql("INSERT INTO t (id) VALUES (2)")

          checkAnswer(sql("SELECT id, region, length(region), label, empty, missing, " +
            "trimmed, trimmed_v FROM t"),
            Seq(1, 2).map(Row(_, "EU ", 3, "US", "   ", null, "EU ", "US ")))
          checkAnswer(sql("SELECT count(DISTINCT region) FROM t"), Row(1L))
          checkAnswer(sql("SELECT region, count(*) FROM t GROUP BY region"), Row("EU ", 2L))
          assert(spark.table("t").schema("region").getExistenceDefaultValue().contains("'EU '"))
          assert(spark.table("t").schema("region").getCurrentDefaultValue().contains("'EU'"))
        }
      }
    }
  }

  Seq("CREATE", "ALTER").foreach { ddl =>
    test(s"CHAR/VARCHAR current defaults use insertion flags with cached metadata: $ddl") {
      withSQLConf(
        SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "false",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
        withTable("t") {
          val columns = "region CHAR(3) DEFAULT 'EU', label VARCHAR(3) DEFAULT 'EU  '"
          if (ddl == "CREATE") {
            sql(s"CREATE TABLE t(id INT, $columns) USING parquet")
          } else {
            sql("CREATE TABLE t(id INT) USING parquet")
            sql("INSERT INTO t VALUES (0)")
            sql(s"ALTER TABLE t ADD COLUMNS ($columns)")
          }
          val historicalRows = if (ddl == "ALTER") {
            Seq(Row(0, "EU ", 3, "EU ", 3))
          } else {
            Seq.empty[Row]
          }
          withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
            sql("INSERT INTO t (id) VALUES (1)")
            sql("INSERT INTO t VALUES (2, 'EU', 'EU  ')")
            val query = "SELECT id, region, length(region), label, length(label) FROM t"
            checkAnswer(sql(query), historicalRows ++
              Seq(1, 2).map(Row(_, "EU", 2, "EU  ", 4)))

            // Serialized schemas retain the SQL but drop cached expressions.
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
            val schema = StructType.fromString(table.schema.json)
            spark.sessionState.catalog.alterTableDataSchema(TableIdentifier("t"), schema)
            spark.catalog.refreshTable("t")
            sql("INSERT INTO t (id) VALUES (3)")
            checkAnswer(sql(query), historicalRows ++
              Seq(1, 2, 3).map(Row(_, "EU", 2, "EU  ", 4)))
          }
        }
      }
    }
  }

  // Cover the overlay, both first-class modes (including legacy overrides), and pure legacy.
  Seq(
    (false, true, false),
    (true, true, false),
    (true, false, true),
    (false, false, false),
    (true, false, false)).foreach { case (legacy, preserve, standard) =>
    test(s"V1 SET DEFAULT follows insertion flags: " +
        s"legacy=$legacy, preserve=$preserve, standard=$standard") {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "false",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
        withTable("t") {
          val identifier = TableIdentifier("t")
          val catalog = spark.sessionState.catalog
          def schema: StructType = catalog.getTableRawMetadata(identifier).schema

          sql("CREATE TABLE t(id INT) USING parquet")
          sql("INSERT INTO t VALUES (0)")
          sql("ALTER TABLE t ADD COLUMNS (" +
            "region CHAR(3) DEFAULT 'US', label VARCHAR(3) DEFAULT 'US')")
          val existenceDefaults = Seq("region", "label").map { name =>
            schema(name).getExistenceDefaultValue()
          }
          assert(existenceDefaults == Seq(Some("'US '"), Some("'US'")))

          // Under preserve-only semantics this returns CHAR(3) with a two-character value.
          // SET must not retain its assigned three-character value for a later legacy INSERT.
          val charDefault = "substring(CAST('EU' AS CHAR(3)), 1, 2)"
          val varcharDefault = "'EU  '"
          val definitionChecks = !legacy || preserve || standard
          withSQLConf(
            SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> legacy.toString,
            SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve.toString,
            SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> standard.toString) {
            sql(s"ALTER TABLE t ALTER COLUMN region SET DEFAULT $charDefault")
            sql(s"ALTER TABLE t ALTER COLUMN label SET DEFAULT $varcharDefault")
            sql("INSERT INTO t (id) VALUES (1)")
          }

          def checkMetadata(hasCurrentDefault: Boolean): Unit = {
            val currentSchema = schema
            Seq("region", "label").zip(existenceDefaults).foreach { case (name, default) =>
              assert(currentSchema(name).getExistenceDefaultValue() == default)
            }
            assert(currentSchema("region").getCurrentDefaultValue() ==
              Option.when(hasCurrentDefault)(charDefault))
            assert(currentSchema("label").getCurrentDefaultValue() ==
              Option.when(hasCurrentDefault)(varcharDefault))
          }
          checkMetadata(hasCurrentDefault = true)

          val originalRows = Seq(
            Row(0, "US ", 3, "US", 2),
            if (definitionChecks) Row(1, "EU ", 3, "EU ", 3)
            else Row(1, "EU", 2, "EU  ", 4))
          // Reverse assignment policy at INSERT, keeping first-class types and read padding off.
          withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> definitionChecks.toString) {
            val query = "SELECT id, region, length(region), label, length(label) FROM t"
            checkAnswer(sql(s"SELECT $charDefault, length($charDefault)"), Row("EU", 2))
            def insertDefaults(firstId: Int): Seq[Row] = {
              sql(s"INSERT INTO t (id) VALUES ($firstId)")
              sql(s"INSERT INTO t VALUES (${firstId + 1}, DEFAULT, DEFAULT), " +
                s"(${firstId + 2}, $charDefault, $varcharDefault)")
              (firstId until firstId + 3).map { id =>
                if (definitionChecks) Row(id, "EU", 2, "EU  ", 4)
                else Row(id, "EU ", 3, "EU ", 3)
              }
            }

            // Exercise the production V1 catalog without first serializing away its expressions.
            val retainedRows = insertDefaults(2)
            checkAnswer(sql(query), originalRows ++ retainedRows)
            checkMetadata(hasCurrentDefault = true)

            // Reload the declared types, dropping expressions without changing CHAR to STRING.
            val reloadedSchema = StructType.fromString(schema.json)
            catalog.alterTableDataSchema(identifier, reloadedSchema)
            spark.catalog.refreshTable("t")
            checkMetadata(hasCurrentDefault = true)
            val reloadedRows = insertDefaults(5)
            checkAnswer(sql(query), originalRows ++ retainedRows ++ reloadedRows)

            sql("ALTER TABLE t ALTER COLUMN region DROP DEFAULT")
            sql("ALTER TABLE t ALTER COLUMN label DROP DEFAULT")
            checkMetadata(hasCurrentDefault = false)
            sql("INSERT INTO t (id) VALUES (8)")
            sql("INSERT INTO t VALUES (9, DEFAULT, DEFAULT)")
            checkAnswer(sql(query), originalRows ++ retainedRows ++ reloadedRows ++
              Seq(Row(8, null, null, null, null), Row(9, null, null, null, null)))
          }
        }
      }
    }
  }

  test("V1 SET DEFAULT follows insertion flags: replace a preexisting legacy current cache") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true",
      SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
      SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
      withTable("t") {
        val identifier = TableIdentifier("t")
        val catalog = spark.sessionState.catalog
        val currentKey = ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY
        sql("CREATE TABLE t(id INT) USING parquet")
        sql("INSERT INTO t VALUES (0)")
        sql("ALTER TABLE t ADD COLUMNS (region CHAR(3) DEFAULT 'US')")

        val originalField = catalog.getTableRawMetadata(identifier).schema("region")
        val existenceDefault = originalField.getExistenceDefaultValue()
        assert(existenceDefault.contains("'US'"))
        val (originalSQL, originalExpression) = originalField.metadata
          .getExpression[Expression](currentKey)
        assert(originalSQL == "'US'")
        assert(originalExpression.exists(_.eval().toString == "US"))

        val charDefault = "substring(CAST('EU' AS CHAR(3)), 1, 2)"
        withSQLConf(
          SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "false",
          SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
          sql(s"ALTER TABLE t ALTER COLUMN region SET DEFAULT $charDefault")
        }
        val updatedField = catalog.getTableRawMetadata(identifier).schema("region")
        assert(updatedField.getCurrentDefaultValue().contains(charDefault))
        assert(updatedField.getExistenceDefaultValue() == existenceDefault)

        // Merging SQL-only metadata must clear the previous US expression as well.
        sql("INSERT INTO t (id) VALUES (1)")
        sql(s"INSERT INTO t VALUES (2, DEFAULT), (3, $charDefault)")
        val query = "SELECT id, region, length(region) FROM t"
        val expectedRows = Seq(Row(0, "US", 2)) ++ Seq(1, 2, 3).map(Row(_, "EU", 2))
        checkAnswer(sql(query), expectedRows)
        assert(updatedField.metadata.getExpression[Expression](currentKey)._2.isEmpty)

        sql("ALTER TABLE t ALTER COLUMN region DROP DEFAULT")
        val droppedField = catalog.getTableRawMetadata(identifier).schema("region")
        assert(droppedField.getCurrentDefaultValue().isEmpty)
        assert(droppedField.getExistenceDefaultValue() == existenceDefault)
        sql("INSERT INTO t (id) VALUES (4)")
        checkAnswer(sql(query), expectedRows :+ Row(4, null, null))
      }
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

  for {
    ddl <- Seq("CREATE", "ALTER")
    legacy <- Seq(false, true)
  } {
    test(s"CHAR late-folded existence defaults use write-side values: $ddl, legacy=$legacy") {
      withSQLConf(
        SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> legacy.toString,
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
        val currentUser = sql("SELECT current_user(), length(current_user())").head()
        val user = currentUser.getString(0)
        val userLength = currentUser.getInt(1)
        assert(userLength < 100)
        val expectedUser = if (legacy) user else user + " " * (100 - userLength)
        val expectedLength = if (legacy) userLength else 100
        withTable("t") {
          if (ddl == "CREATE") {
            sql("CREATE TABLE t(id INT, region CHAR(100) DEFAULT current_user()) USING parquet")
          } else {
            sql("CREATE TABLE t(id INT) USING parquet")
            sql("INSERT INTO t VALUES (1)")
            sql("ALTER TABLE t ADD COLUMNS (region CHAR(100) DEFAULT current_user())")
          }
          sql("INSERT INTO t (id) VALUES (2)")
          val ids = if (ddl == "ALTER") Seq(1, 2) else Seq(2)
          val query = "SELECT id, region, length(region) FROM t"
          val expectedRows = ids.map(Row(_, expectedUser, expectedLength))
          checkAnswer(sql(query), expectedRows)
          checkAnswer(sql("SELECT count(DISTINCT region) FROM t"), Row(1L))
          checkAnswer(sql("SELECT region, count(*) FROM t GROUP BY region"),
            Row(expectedUser, ids.size.toLong))

          val field = spark.table("t").schema("region")
          assert(field.getCurrentDefaultValue().contains("current_user()"))
          val existenceSql = field.getExistenceDefaultValue().get
          checkAnswer(sql(s"SELECT $existenceSql, length($existenceSql)"),
            Row(expectedUser, expectedLength))

          sql("ALTER TABLE t ALTER COLUMN region SET DEFAULT 'EU'")
          val updatedField = spark.table("t").schema("region")
          assert(updatedField.getCurrentDefaultValue().contains("'EU'"))
          assert(updatedField.getExistenceDefaultValue().contains(existenceSql))
          sql("INSERT INTO t (id) VALUES (3)")
          val newDefault = if (legacy) "EU" else "EU" + " " * 98
          checkAnswer(sql(query), expectedRows :+ Row(3, newDefault, if (legacy) 2 else 100))
        }
      }
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
