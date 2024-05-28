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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.SQLViewSuite
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.{NullType, StructType}
import org.apache.spark.tags.SlowHiveTest

/**
 * A test suite for Hive view related functionality.
 */
@SlowHiveTest
class HiveSQLViewSuite extends SQLViewSuite with TestHiveSingleton {
  import testImplicits._

  test("create a permanent/temp view using a hive, built-in, and permanent user function") {
    val permanentFuncName = "myUpper"
    val permanentFuncClass =
      classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper].getCanonicalName
    val builtInFuncNameInLowerCase = "abs"
    val builtInFuncNameInMixedCase = "aBs"
    val hiveFuncName = "histogram_numeric"

    withUserDefinedFunction(permanentFuncName -> false) {
      sql(s"CREATE FUNCTION $permanentFuncName AS '$permanentFuncClass'")
      withTable("tab1") {
        (1 to 10).map(i => (s"$i", i)).toDF("str", "id").write.saveAsTable("tab1")
        Seq("VIEW", "TEMPORARY VIEW").foreach { viewMode =>
          withView("view1") {
            sql(
              s"""
                 |CREATE $viewMode view1
                 |AS SELECT
                 |$permanentFuncName(str) as myUpper,
                 |$builtInFuncNameInLowerCase(id) as abs_lower,
                 |$builtInFuncNameInMixedCase(id) as abs_mixed,
                 |$hiveFuncName(id, 5) over() as func
                 |FROM tab1
               """.stripMargin)
            checkAnswer(sql("select count(*) FROM view1"), Row(10))
          }
        }
      }
    }
  }

  test("create a permanent/temp view using a temporary function") {
    val tempFunctionName = "temp"
    val functionClass =
      classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper].getCanonicalName
    withUserDefinedFunction(tempFunctionName -> true) {
      sql(s"CREATE TEMPORARY FUNCTION $tempFunctionName AS '$functionClass'")
      withView("view1") {
        withTempView("tempView1") {
          withTable("tab1") {
            (1 to 10).map(i => s"$i").toDF("id").write.saveAsTable("tab1")

            // temporary view
            sql(s"CREATE TEMPORARY VIEW tempView1 AS SELECT $tempFunctionName(id) from tab1")
            checkAnswer(sql("select count(*) FROM tempView1"), Row(10))

            // permanent view
            val e = intercept[AnalysisException] {
              sql(s"CREATE VIEW view1 AS SELECT $tempFunctionName(id) from tab1")
            }
            checkError(
              exception = e,
              errorClass = "INVALID_TEMP_OBJ_REFERENCE",
              parameters = Map(
                "obj" -> "VIEW",
                "objName" -> s"`$SESSION_CATALOG_NAME`.`default`.`view1`",
                "tempObj" -> "FUNCTION",
                "tempObjName" -> s"`$tempFunctionName`"))
          }
        }
      }
    }
  }

  test("SPARK-14933 - create view from hive parquet table") {
    withTable("t_part") {
      withView("v_part") {
        spark.sql("create table t_part stored as parquet as select 1 as a, 2 as b")
        spark.sql("create view v_part as select * from t_part")
        checkAnswer(
          sql("select * from t_part"),
          sql("select * from v_part"))
      }
    }
  }

  test("SPARK-14933 - create view from hive orc table") {
    withTable("t_orc") {
      withView("v_orc") {
        spark.sql("create table t_orc stored as orc as select 1 as a, 2 as b")
        spark.sql("create view v_orc as select * from t_orc")
        checkAnswer(
          sql("select * from t_orc"),
          sql("select * from v_orc"))
      }
    }
  }

  test("make sure we can resolve view created by old version of Spark") {
    withTable("hive_table") {
      withView("old_view") {
        spark.sql("CREATE TABLE hive_table AS SELECT 1 AS a, 2 AS b")
        // The views defined by older versions of Spark(before 2.2) will have empty view default
        // database name, and all the relations referenced in the viewText will have database part
        // defined.
        val view = CatalogTable(
          identifier = TableIdentifier("old_view"),
          tableType = CatalogTableType.VIEW,
          storage = CatalogStorageFormat.empty,
          schema = new StructType().add("a", "int").add("b", "int"),
          viewText = Some("SELECT `gen_attr_0` AS `a`, `gen_attr_1` AS `b` FROM (SELECT " +
            "`gen_attr_0`, `gen_attr_1` FROM (SELECT `a` AS `gen_attr_0`, `b` AS " +
            "`gen_attr_1` FROM hive_table) AS gen_subquery_0) AS hive_table")
        )
        hiveContext.sessionState.catalog.createTable(view, ignoreIfExists = false)
        val df = sql("SELECT * FROM old_view")
        // Check the output rows.
        checkAnswer(df, Row(1, 2))
        // Check the output schema.
        assert(DataTypeUtils.sameType(df.schema, view.schema))
      }
    }
  }

  test("SPARK-20680: Add HiveVoidType to compatible with Hive void type") {
    withView("v1") {
      sql("create view v1 as select null as c")
      val df = sql("select * from v1")
      assert(df.schema.fields.head.dataType == NullType)
      checkAnswer(
        df,
        Row(null)
      )

      sql("alter view v1 as select null as c1, 1 as c2")
      val df2 = sql("select * from v1")
      assert(df2.schema.fields.head.dataType == NullType)
      checkAnswer(
        df2,
        Row(null, 1)
      )
    }
  }

  test("SPARK-35792: ignore optimization configs used in RelationConversions") {
    withTable("t_orc") {
      withView("v_orc") {
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") {
          spark.sql("create table t_orc stored as orc as select 1 as a, 2 as b")
          spark.sql("create view v_orc as select * from t_orc")
        }
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
          val relationInTable = sql("select * from t_orc").queryExecution.analyzed.collect {
            case r: HiveTableRelation => r
          }.headOption
          val relationInView = sql("select * from v_orc").queryExecution.analyzed.collect {
            case r: HiveTableRelation => r
          }.headOption
          assert(relationInTable.isDefined)
          assert(relationInView.isDefined)
        }
      }
    }
  }

  test("hive partitioned view is not supported") {
    withTable("test") {
      withView("v1") {
        sql(
          s"""
             |CREATE TABLE test (c1 INT, c2 STRING)
             |PARTITIONED BY (
             |  p1 BIGINT COMMENT 'bla',
             |  p2 STRING )
           """.stripMargin)

        createRawHiveTable(
          s"""
             |CREATE VIEW v1
             |PARTITIONED ON (p1, p2)
             |AS SELECT * from test
           """.stripMargin
        )

        checkError(
          exception = intercept[AnalysisException] {
            sql("SHOW CREATE TABLE v1")
          },
          errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_FEATURE",
          parameters = Map(
            "table" -> s"`$SESSION_CATALOG_NAME`.`default`.`v1`",
            "features" -> " - partitioned view"
          )
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql("SHOW CREATE TABLE v1 AS SERDE")
          },
          errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_FEATURE",
          parameters = Map(
            "table" -> s"`$SESSION_CATALOG_NAME`.`default`.`v1`",
            "features" -> " - partitioned view"
          )
        )
      }
    }
  }

  private def createRawHiveTable(ddl: String): Unit = {
    hiveContext.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
      .client.runSqlHive(ddl)
  }
}
