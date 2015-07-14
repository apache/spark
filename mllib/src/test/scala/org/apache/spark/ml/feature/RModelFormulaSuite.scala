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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RFormulaModelSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    ParamsSuite.checkParams(new RModelFormula())
  }

  test("parse simple formulas") {
    def check(formula: String, response: String, terms: Seq[String]) {
      new RModelFormula().setFormula(formula)
      val parsed = RFormulaParser.parse(formula)
      assert(parsed.response == response)
      assert(parsed.terms == terms)
    }
    check("y ~ x", "y", Seq("x"))
    check("y ~   ._foo  ", "y", Seq("._foo"))
    check("resp ~ A_VAR + B + c123", "resp", Seq("A_VAR", "B", "c123"))
  }

  test("transform numeric data") {
    val formula = new RModelFormula().setFormula("id ~ v1 + v2")
    val original = sqlContext.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")
    val result = formula.transform(original)
    val resultSchema = formula.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        (0, 1.0, 3.0, Vectors.dense(Array(1.0, 3.0)), 0.0),
        (2, 2.0, 5.0, Vectors.dense(Array(2.0, 5.0)), 2.0))
      ).toDF("id", "v1", "v2", "features", "label")
    assert(result.schema.toString == resultSchema.toString)
    assert(resultSchema.toString == expected.schema.toString)
    assert(
      result.collect().map(_.toString).sorted.mkString(",") ==
      expected.collect().map(_.toString).sorted.mkString(","))
  }

  test("transform string label") {
    val formula = new RModelFormula().setFormula("name ~ id")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo"), (2, "bar"), (3, "bar"))).toDF("id", "name")
    val result = formula.transform(original)
    val resultSchema = formula.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", Vectors.dense(Array(1.0)), 1.0),
        (2, "bar", Vectors.dense(Array(2.0)), 0.0),
        (3, "bar", Vectors.dense(Array(3.0)), 0.0))
      ).toDF("id", "name", "features", "label")
    assert(result.schema.toString == resultSchema.toString)
    assert(
      result.collect().map(_.toString).sorted.mkString(",") ==
      expected.collect().map(_.toString).sorted.mkString(","))
  }
}
