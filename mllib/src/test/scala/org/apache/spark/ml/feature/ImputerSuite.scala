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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}

class ImputerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Imputer for Double with default missing Value NaN") {
    val df = spark.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, 1.0, 1.0, 1.0),
      (2, 3.0, 3.0, 3.0),
      (3, 4.0, 4.0, 4.0),
      (4, Double.NaN, 2.25, 3.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer should handle NaNs when computing surrogate value, if missingValue is not NaN") {
    val df = spark.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN),
      (3, -1.0, 2.0, 3.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
      .setMissingValue(-1.0)
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer for Float with missing Value -1.0") {
    val df = spark.createDataFrame( Seq(
      (0, 1.0F, 1.0F, 1.0F),
      (1, 3.0F, 3.0F, 3.0F),
      (2, 10.0F, 10.0F, 10.0F),
      (3, 10.0F, 10.0F, 10.0F),
      (4, -1.0F, 6.0F, 3.0F)
    )).toDF("id", "value", "expected_mean", "expected_median")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
      .setMissingValue(-1)
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer should impute null as well as 'missingValue'") {
    val df = spark.createDataFrame( Seq(
      (0, 4.0, 4.0, 4.0),
      (1, 10.0, 10.0, 10.0),
      (2, 10.0, 10.0, 10.0),
      (3, Double.NaN, 8.0, 10.0),
      (4, -1.0, 8.0, 10.0)
    )).toDF("id", "value", "expected_mean", "expected_median")
    val df2 = df.selectExpr("*", "IF(value=-1.0, null, value) as nullable_value")
    val imputer = new Imputer().setInputCol("nullable_value").setOutputCol("out")
    ImputerSuite.iterateStrategyTest(imputer, df2)
  }


  test("Imputer throws exception when surrogate cannot be computed") {
    val df = spark.createDataFrame( Seq(
      (0, Double.NaN, 1.0, 1.0),
      (1, Double.NaN, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN)
    )).toDF("id", "value", "expected_mean", "expected_median")
    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCol("value").setOutputCol("out").setStrategy(strategy)
      intercept[SparkException] {
        val model = imputer.fit(df)
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
    val spark = this.spark
    import spark.implicits._
    val surrogateDF = Seq(1.234).toDF("myInputCol")

    val instance = new ImputerModel(
      "myImputer", surrogateDF)
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.surrogateDF.collect() === instance.surrogateDF.collect())
  }

}

object ImputerSuite{

  /**
   * Imputation strategy. Available options are ["mean", "median"].
   * @param df DataFrame with columns "id", "value", "expected_mean", "expected_median"
   */
  def iterateStrategyTest(imputer: Imputer, df: DataFrame): Unit = {
    Seq("mean", "median").foreach { strategy =>
      imputer.setStrategy(strategy)
      val model = imputer.fit(df)
      model.transform(df).select("expected_" + strategy, "out").collect().foreach {
        case Row(exp: Float, out: Float) =>
          assert((exp.isNaN && out.isNaN) || (exp == out),
            s"Imputed values differ. Expected: $exp, actual: $out")
        case Row(exp: Double, out: Double) =>
          assert((exp.isNaN && out.isNaN) || (exp ~== out absTol 1e-5),
            s"Imputed values differ. Expected: $exp, actual: $out")
      }
    }
  }
}
