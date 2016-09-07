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
import org.apache.spark.ml.util.{DefaultReadWriteTest}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.Row

class ImputerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Imputer for Double with default missing Value NaN") {
    val df = sqlContext.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, 1.0, 1.0, 1.0),
      (2, 3.0, 3.0, 3.0),
      (3, 4.0, 4.0, 4.0),
      (4, Double.NaN, 2.25, 1.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCol("value").setOutputCol("out").setStrategy(strategy)
      val model = imputer.fit(df)
      model.transform(df).select("expected_" + strategy, "out").collect().foreach {
       case Row(exp: Double, out: Double) =>
          assert(exp ~== out absTol 1e-5, s"Imputed values differ. Expected: $exp, actual: $out")
      }
    }
  }

  test("Imputer should handle NaNs when computing surrogate value, if missingValue is not NaN") {
    val df = sqlContext.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN),
      (3, -1.0, 2.0, 3.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCol("value").setOutputCol("out").setStrategy(strategy)
        .setMissingValue(-1.0)
      val model = imputer.fit(df)
      model.transform(df).select("expected_" + strategy, "out").collect().foreach {
        case Row(exp: Double, out: Double) =>
          assert((exp.isNaN && out.isNaN) || (exp ~== out absTol 1e-5),
            s"Imputed values differ. Expected: $exp, actual: $out")
      }
    }
  }

  test("Imputer for Float with missing Value -1.0") {
    val df = sqlContext.createDataFrame( Seq(
      (0, 1.0F, 1.0F, 1.0F),
      (1, 3.0F, 3.0F, 3.0F),
      (2, 10.0F, 10.0F, 10.0F),
      (3, 10.0F, 10.0F, 10.0F),
      (4, -1.0F, 6.0F, 3.0F)
    )).toDF("id", "value", "expected_mean", "expected_median")

    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCol("value").setOutputCol("out").setStrategy(strategy)
        .setMissingValue(-1)
      val model = imputer.fit(df)
      val result = model.transform(df)
      model.transform(df).select("expected_" + strategy, "out").collect().foreach {
        case Row(exp: Float, out: Float) =>
          assert(exp == out, s"Imputed values differ. Expected: $exp, actual: $out")
      }
    }
  }

  test("Imputer should impute null as well as 'missingValue'") {
    val df = sqlContext.createDataFrame( Seq(
      (0, 4.0, 4.0, 4.0),
      (1, 10.0, 10.0, 10.0),
      (2, 10.0, 10.0, 10.0),
      (3, Double.NaN, 8.0, 10.0),
      (4, -1.0, 8.0, 10.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    val df2 = df.selectExpr("*", "IF(value=-1.0, null, value) as nullable_value")
    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCol("nullable_value").setOutputCol("out")
        .setStrategy(strategy)
      val model = imputer.fit(df2)
      model.transform(df2).select("expected_" + strategy, "out").collect().foreach {
        case Row(exp: Double, out: Double) =>
          assert(exp ~== out absTol 1e-5, s"Imputed values differ. Expected: $exp, actual: $out")
      }
    }
  }

  test("Imputer read/write") {
    val t = new Imputer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMissingValue(-1.0)
    testDefaultReadWrite(t)
  }

  test("ImputerModel read/write") {
    val instance = new ImputerModel(
      "myImputer", 1.234)
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.surrogate === instance.surrogate)
  }

}
