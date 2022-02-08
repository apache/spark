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

package org.apache.spark.sql.errors

import org.apache.spark.sql.{AnalysisException, Dataset, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, UpCast}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions.{grouping, grouping_id}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.NumericType

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class QueryCompilationErrorsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("CANNOT_UP_CAST_DATATYPE: invalid upcast data type") {
    val msg1 = intercept[AnalysisException] {
      sql("select 'value1' as a, 1L as b").as[StringIntClass]
    }.message
    assert(msg1 ===
      s"""
         |Cannot up cast b from bigint to int.
         |The type path of the target object is:
         |- field (class: "scala.Int", name: "b")
         |- root class: "org.apache.spark.sql.errors.StringIntClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")

    val msg2 = intercept[AnalysisException] {
      sql("select 1L as a," +
        " named_struct('a', 'value1', 'b', cast(1.0 as decimal(38,18))) as b")
        .as[ComplexClass]
    }.message
    assert(msg2 ===
      s"""
         |Cannot up cast b.`b` from decimal(38,18) to bigint.
         |The type path of the target object is:
         |- field (class: "scala.Long", name: "b")
         |- field (class: "org.apache.spark.sql.errors.StringLongClass", name: "b")
         |- root class: "org.apache.spark.sql.errors.ComplexClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")
  }

  test("UNSUPPORTED_FEATURE: UpCast only support DecimalType as AbstractDataType") {
    val df = sql("select 1 as value")

    val msg = intercept[AnalysisException] {
      val plan = Project(
        Seq(Alias(UpCast(UnresolvedAttribute("value"), NumericType), "value")()),
        df.logicalPlan)

      Dataset.ofRows(spark, plan)
    }.message
    assert(msg.matches("The feature is not supported: " +
      "UpCast only support DecimalType as AbstractDataType yet," +
      """ but got: org.apache.spark.sql.types.NumericType\$\@\w+"""))
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: filter with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq("grouping", "grouping_id").foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max"))
          .filter(s"$grouping(CustomerId)=17850")
      }
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
    }
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: Sort with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq(grouping("CustomerId"), grouping_id("CustomerId")).foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max")).
          sort(grouping)
      }
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    }
  }
}
