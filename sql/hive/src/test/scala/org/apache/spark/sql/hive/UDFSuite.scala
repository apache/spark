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

  import hiveContext.implicits._

  private[this] val functionName = "myUpper"
  private[this] val functionClass =
    classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper].getCanonicalName

  private var testDF: DataFrame = null
  private[this] val testTableName = "testDF_UDFSuite"
  private var expectedDF: DataFrame = null

  override def beforeAll(): Unit = {
    sql("USE default")

    testDF = (1 to 10).map(i => s"sTr$i").toDF("value")
    testDF.registerTempTable(testTableName)
    expectedDF = (1 to 10).map(i => s"STR$i").toDF("value")
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    sql("USE default")
    super.afterEach()
  }

  test("UDF case insensitive") {
    hiveContext.udf.register("random0", () => { Math.random() })
    hiveContext.udf.register("RANDOM1", () => { Math.random() })
    hiveContext.udf.register("strlenScala", (_: String).length + (_: Int))
    assert(hiveContext.sql("SELECT RANDOM0() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(hiveContext.sql("SELECT RANDOm1() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(hiveContext.sql("SELECT strlenscala('test', 1) FROM src LIMIT 1").head().getInt(0) === 5)
  }

  test("temporary function: create and drop") {
    withUserDefinedFunction(functionName -> true) {
      intercept[AnalysisException] {
        sql(s"CREATE TEMPORARY FUNCTION default.$functionName AS '$functionClass'")
      }
      sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$functionClass'")
      checkAnswer(
        sql(s"SELECT myupper(value) from $testTableName"),
        expectedDF
      )
      intercept[AnalysisException] {
        sql(s"DROP TEMPORARY FUNCTION default.$functionName")
      }
    }
  }

  test("permanent function: create and drop without specifying db name") {
    withUserDefinedFunction(functionName -> false) {
      sql(s"CREATE FUNCTION $functionName AS '$functionClass'")
      checkAnswer(
        sql("SHOW functions like '.*upper'"),
        Row("default.myupper")
      )
      checkAnswer(
        sql(s"SELECT myupper(value) from $testTableName"),
        expectedDF
      )
      assert(sql("SHOW functions").collect().map(_.getString(0)).contains("default.myupper"))
    }
  }

  test("permanent function: create and drop with a db name") {
    // For this block, drop function command uses functionName as the function name.
    withUserDefinedFunction(functionName.toUpperCase -> false) {
      sql(s"CREATE FUNCTION default.$functionName AS '$functionClass'")
      // TODO: Re-enable it after can distinguish qualified and unqualified function name
      // in SessionCatalog.lookupFunction.
      // checkAnswer(
      //  sql(s"SELECT default.myuPPer(value) from $testTableName"),
      //  expectedDF
      // )
      checkAnswer(
        sql(s"SELECT myuppER(value) from $testTableName"),
        expectedDF
      )
      checkAnswer(
        sql(s"SELECT default.MYupper(value) from $testTableName"),
        expectedDF
      )
    }

    // For this block, drop function command uses default.functionName as the function name.
    withUserDefinedFunction(s"DEfault.$functionName" -> false) {
      sql(s"CREATE FUNCTION dEFault.$functionName AS '$functionClass'")
      checkAnswer(
        sql(s"SELECT myUpper(value) from $testTableName"),
        expectedDF
      )
    }
  }

  test("permanent function: create and drop a function in another db") {
    // For this block, drop function command uses functionName as the function name.
    withTempDatabase { dbName =>
      withUserDefinedFunction(functionName -> false) {
        sql(s"CREATE FUNCTION $dbName.$functionName AS '$functionClass'")
        // TODO: Re-enable it after can distinguish qualified and unqualified function name
        // checkAnswer(
        //  sql(s"SELECT $dbName.myuPPer(value) from $testTableName"),
        //  expectedDF
        // )

        checkAnswer(
          sql(s"SHOW FUNCTIONS like $dbName.myupper"),
          Row(s"$dbName.myupper")
        )

        sql(s"USE $dbName")

        checkAnswer(
          sql(s"SELECT myuppER(value) from $testTableName"),
          expectedDF
        )

        sql(s"USE default")

        checkAnswer(
          sql(s"SELECT $dbName.MYupper(value) from $testTableName"),
          expectedDF
        )

        sql(s"USE $dbName")
      }

      sql(s"USE default")

      // For this block, drop function command uses default.functionName as the function name.
      withUserDefinedFunction(s"$dbName.$functionName" -> false) {
        sql(s"CREATE FUNCTION $dbName.$functionName AS '$functionClass'")
        // TODO: Re-enable it after can distinguish qualified and unqualified function name
        // checkAnswer(
        //  sql(s"SELECT $dbName.myupper(value) from $testTableName"),
        //  expectedDF
        // )

        sql(s"USE $dbName")

        assert(sql("SHOW functions").collect().map(_.getString(0)).contains(s"$dbName.myupper"))
        checkAnswer(
          sql(s"SELECT myupper(value) from $testTableName"),
          expectedDF
         )

        sql(s"USE default")
      }
    }
  }
}
