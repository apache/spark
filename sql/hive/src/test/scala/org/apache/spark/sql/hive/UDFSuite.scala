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

package org.apache.spark.sql.hive

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

case class FunctionResult(f1: String, f2: String)

/**
 * A test suite for UDF related functionalities. Because Hive metastore is
 * case insensitive, database names and function names have both upper case
 * letters and lower case letters.
 */
class UDFSuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton
  with BeforeAndAfterEach {

  import spark.implicits._

  private[this] val functionName = "myUPper"
  private[this] val functionNameUpper = "MYUPPER"
  private[this] val functionNameLower = "myupper"

  private[this] val functionClass =
    classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper].getCanonicalName

  private var testDF: DataFrame = null
  private[this] val testTableName = "testDF_UDFSuite"
  private var expectedDF: DataFrame = null

  override def beforeAll(): Unit = {
    sql("USE default")

    testDF = (1 to 10).map(i => s"sTr$i").toDF("value")
    testDF.createOrReplaceTempView(testTableName)
    expectedDF = (1 to 10).map(i => s"STR$i").toDF("value")
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    sql("USE default")
    super.afterEach()
  }

  test("UDF case insensitive") {
    spark.udf.register("random0", () => { Math.random() })
    spark.udf.register("RANDOM1", () => { Math.random() })
    spark.udf.register("strlenScala", (_: String).length + (_: Int))
    assert(sql("SELECT RANDOM0() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(sql("SELECT RANDOm1() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(sql("SELECT strlenscala('test', 1) FROM src LIMIT 1").head().getInt(0) === 5)
  }

  test("temporary function: create and drop") {
    withUserDefinedFunction(functionName -> true) {
      intercept[ParseException] {
        sql(s"CREATE TEMPORARY FUNCTION default.$functionName AS '$functionClass'")
      }
      sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$functionClass'")
      checkAnswer(
        sql(s"SELECT $functionNameLower(value) from $testTableName"),
        expectedDF
      )
      intercept[ParseException] {
        sql(s"DROP TEMPORARY FUNCTION default.$functionName")
      }
    }
  }

  test("permanent function: create and drop without specifying db name") {
    withUserDefinedFunction(functionName -> false) {
      sql(s"CREATE FUNCTION $functionName AS '$functionClass'")
      checkAnswer(
        sql("SHOW functions like '.*upper'"),
        Row(s"$SESSION_CATALOG_NAME.default.$functionNameLower")
      )
      checkAnswer(
        sql(s"SELECT $functionName(value) from $testTableName"),
        expectedDF
      )
      assert(
        sql("SHOW functions").collect()
          .map(_.getString(0))
          .contains(s"$SESSION_CATALOG_NAME.default.$functionNameLower"))
    }
  }

  test("permanent function: create and drop with a db name") {
    // For this block, drop function command uses functionName as the function name.
    withUserDefinedFunction(functionNameUpper -> false) {
      sql(s"CREATE FUNCTION default.$functionName AS '$functionClass'")
      // TODO: Re-enable it after can distinguish qualified and unqualified function name
      // in SessionCatalog.lookupFunction.
      // checkAnswer(
      //  sql(s"SELECT default.myuPPer(value) from $testTableName"),
      //  expectedDF
      // )
      checkAnswer(
        sql(s"SELECT $functionName(value) from $testTableName"),
        expectedDF
      )
      checkAnswer(
        sql(s"SELECT default.$functionName(value) from $testTableName"),
        expectedDF
      )
    }

    // For this block, drop function command uses default.functionName as the function name.
    withUserDefinedFunction(s"DEfault.$functionNameLower" -> false) {
      sql(s"CREATE FUNCTION dEFault.$functionName AS '$functionClass'")
      checkAnswer(
        sql(s"SELECT $functionNameUpper(value) from $testTableName"),
        expectedDF
      )
    }
  }

  test("permanent function: create and drop a function in another db") {
    // For this block, drop function command uses functionName as the function name.
    withTempDatabase { dbName =>
      withUserDefinedFunction(functionName -> false) {
        sql(s"CREATE FUNCTION $dbName.$functionName AS '$functionClass'")
        checkAnswer(
          sql(s"SELECT $dbName.$functionName(value) from $testTableName"),
          expectedDF
        )

        checkAnswer(
          sql(s"SHOW FUNCTIONS like $dbName.$functionNameUpper"),
          Row(s"$SESSION_CATALOG_NAME.$dbName.$functionNameLower")
        )

        sql(s"USE $dbName")

        checkAnswer(
          sql(s"SELECT $functionName(value) from $testTableName"),
          expectedDF
        )

        sql(s"USE default")

        checkAnswer(
          sql(s"SELECT $dbName.$functionName(value) from $testTableName"),
          expectedDF
        )

        sql(s"USE $dbName")
      }

      sql(s"USE default")

      // For this block, drop function command uses default.functionName as the function name.
      withUserDefinedFunction(s"$dbName.$functionNameUpper" -> false) {
        sql(s"CREATE FUNCTION $dbName.$functionName AS '$functionClass'")
        checkAnswer(
          sql(s"SELECT $dbName.$functionName(value) from $testTableName"),
          expectedDF
        )

        sql(s"USE $dbName")

        assert(
          sql("SHOW functions").collect()
            .map(_.getString(0))
            .contains(s"$SESSION_CATALOG_NAME.$dbName.$functionNameLower"))
        checkAnswer(
          sql(s"SELECT $functionNameLower(value) from $testTableName"),
          expectedDF
         )

        sql(s"USE default")
      }
    }
  }

  test("SPARK-21318: The correct exception message should be thrown " +
    "if a UDF/UDAF has already been registered") {
    val functionName = "empty"
    val functionClass = classOf[org.apache.spark.sql.hive.execution.UDAFEmpty].getCanonicalName

    withUserDefinedFunction(functionName -> false) {
      sql(s"CREATE FUNCTION $functionName AS '$functionClass'")

      val e = intercept[AnalysisException] {
        sql(s"SELECT $functionName(value) from $testTableName")
      }

      assert(e.getMessage.contains("Can not get an evaluator of the empty UDAF"))
    }
  }

  test("check source for hive UDF") {
    withUserDefinedFunction(functionName -> false) {
      sql(s"CREATE FUNCTION $functionName AS '$functionClass'")
      val info = spark.sessionState.catalog.lookupFunctionInfo(
        FunctionIdentifier(functionName, Some("default")))
      assert(info.getSource == "hive")
    }
  }
}
