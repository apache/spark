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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.hive.execution.{StringCaseClass, UDFToListInt}

/**
 * The class contains tests for the `SHOW FUNCTIONS` command to check V1 Hive external
 * table catalog.
 */
class ShowFunctionsSuite extends v1.ShowFunctionsSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowFunctionsSuiteBase].commandVersion

  test("Show persistent functions") {
    import spark.implicits._
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    withTempView("inputTable") {
      testData.createOrReplaceTempView("inputTable")
      withUserDefinedFunction("testUDFToListInt" -> false) {
        val numFunc = spark.catalog.listFunctions().count()
        sql(s"CREATE FUNCTION testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
        assert(spark.catalog.listFunctions().count() == numFunc + 1)
        checkAnswer(
          sql("SELECT testUDFToListInt(s) FROM inputTable"),
          Seq(Row(Seq(1, 2, 3))))
        assert(sql("show functions").count() ==
          numFunc + FunctionRegistry.builtinOperators.size + 1)
        assert(spark.catalog.listFunctions().count() == numFunc + 1)

        withDatabase("db2") {
          sql("CREATE DATABASE db2")
          sql(s"CREATE FUNCTION db2.testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
          checkAnswer(
            sql("SHOW FUNCTIONS IN db2 LIKE 'testUDF*'"),
            Seq(Row("db2.testudftolistint")))
        }
      }
    }
  }
}
