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

class RFormulaSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    ParamsSuite.checkParams(new RFormula())
  }

  test("transform numeric data") {
    val formula = new RFormula().setFormula("id ~ v1 + v2")
    val original = sqlContext.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")
    val result = formula.transform(original)
    val resultSchema = formula.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        (0, 1.0, 3.0, Vectors.dense(Array(1.0, 3.0)), 0.0),
        (2, 2.0, 5.0, Vectors.dense(Array(2.0, 5.0)), 2.0))
      ).toDF("id", "v1", "v2", "features", "label")
    // TODO(ekl) make schema comparisons ignore metadata, to avoid .toString
    assert(result.schema.toString == resultSchema.toString)
    assert(resultSchema == expected.schema)
    assert(result.collect().toSeq == expected.collect().toSeq)
  }

  test("features column already exists") {
    val formula = new RFormula().setFormula("y ~ x").setFeaturesCol("x")
    val original = sqlContext.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "y")
    intercept[IllegalArgumentException] {
      formula.transformSchema(original.schema)
    }
    intercept[IllegalArgumentException] {
      formula.transform(original)
    }
  }

  test("label column already exists") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = sqlContext.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "y")
    val resultSchema = formula.transformSchema(original.schema)
    assert(resultSchema.length == 3)
    assert(resultSchema.toString == formula.transform(original).schema.toString)
  }

  test("label column already exists but is not double type") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = sqlContext.createDataFrame(Seq((0, 1), (2, 2))).toDF("x", "y")
    intercept[IllegalArgumentException] {
      formula.transformSchema(original.schema)
    }
    intercept[IllegalArgumentException] {
      formula.transform(original)
    }
  }

// TODO(ekl) enable after we implement string label support
//  test("transform string label") {
//    val formula = new RFormula().setFormula("name ~ id")
//    val original = sqlContext.createDataFrame(
//      Seq((1, "foo"), (2, "bar"), (3, "bar"))).toDF("id", "name")
//    val result = formula.transform(original)
//    val resultSchema = formula.transformSchema(original.schema)
//    val expected = sqlContext.createDataFrame(
//      Seq(
//        (1, "foo", Vectors.dense(Array(1.0)), 1.0),
//        (2, "bar", Vectors.dense(Array(2.0)), 0.0),
//        (3, "bar", Vectors.dense(Array(3.0)), 0.0))
//      ).toDF("id", "name", "features", "label")
//    assert(result.schema.toString == resultSchema.toString)
//    assert(result.collect().toSeq == expected.collect().toSeq)
//  }
}
