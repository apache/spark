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
      (0, 1.0, 4.0, 1.0, 1.0, 4.0, 4.0),
      (1, 11.0, 12.0, 11.0, 11.0, 12.0, 12.0),
      (2, 3.0, Double.NaN, 3.0, 3.0, 10.0, 12.0),
      (3, Double.NaN, 14.0, 5.0, 3.0, 14.0, 14.0)
    )).toDF("id", "value1", "value2", "expected_mean_value1", "expected_median_value1",
      "expected_mean_value2", "expected_median_value2")
    val imputer = new Imputer()
      .setInputCols(Array("value1", "value2"))
      .setOutputCols(Array("out1", "out2"))
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer should handle NaNs when computing surrogate value, if missingValue is not NaN") {
    val df = spark.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN),
      (3, -1.0, 2.0, 3.0)
    )).toDF("id", "value", "expected_mean_value", "expected_median_value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
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
    )).toDF("id", "value", "expected_mean_value", "expected_median_value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
      .setMissingValue(-1)
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer should impute null as well as 'missingValue'") {
    val rawDf = spark.createDataFrame( Seq(
      (0, 4.0, 4.0, 4.0),
      (1, 10.0, 10.0, 10.0),
      (2, 10.0, 10.0, 10.0),
      (3, Double.NaN, 8.0, 10.0),
      (4, -1.0, 8.0, 10.0)
    )).toDF("id", "rawValue", "expected_mean_value", "expected_median_value")
    val df = rawDf.selectExpr("*", "IF(rawValue=-1.0, null, rawValue) as value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
    ImputerSuite.iterateStrategyTest(imputer, df)
  }

  test("Imputer throws exception when surrogate cannot be computed") {
    val df = spark.createDataFrame( Seq(
      (0, Double.NaN, 1.0, 1.0),
      (1, Double.NaN, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN)
    )).toDF("id", "value", "expected_mean_value", "expected_median_value")
    Seq("mean", "median").foreach { strategy =>
      val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
        .setStrategy(strategy)
      withClue("Imputer should fail all the values are invalid") {
        val e: SparkException = intercept[SparkException] {
          val model = imputer.fit(df)
        }
        assert(e.getMessage.contains("surrogate cannot be computed"))
      }
    }
  }

  test("Imputer input & output column validation") {
    val df = spark.createDataFrame( Seq(
      (0, 1.0, 1.0, 1.0),
      (1, Double.NaN, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN)
    )).toDF("id", "value1", "value2", "value3")
    Seq("mean", "median").foreach { strategy =>
      withClue("Imputer should fail if inputCols and outputCols are different length") {
        val e: IllegalArgumentException = intercept[IllegalArgumentException] {
          val imputer = new Imputer().setStrategy(strategy)
            .setInputCols(Array("value1", "value2"))
            .setOutputCols(Array("out1"))
          val model = imputer.fit(df)
        }
        assert(e.getMessage.contains("should have the same length"))
      }

      withClue("Imputer should fail if inputCols contains duplicates") {
        val e: IllegalArgumentException = intercept[IllegalArgumentException] {
          val imputer = new Imputer().setStrategy(strategy)
            .setInputCols(Array("value1", "value1"))
            .setOutputCols(Array("out1", "out2"))
          val model = imputer.fit(df)
        }
        assert(e.getMessage.contains("inputCols contains duplicates"))
      }

      withClue("Imputer should fail if outputCols contains duplicates") {
        val e: IllegalArgumentException = intercept[IllegalArgumentException] {
          val imputer = new Imputer().setStrategy(strategy)
            .setInputCols(Array("value1", "value2"))
            .setOutputCols(Array("out1", "out1"))
          val model = imputer.fit(df)
        }
        assert(e.getMessage.contains("outputCols contains duplicates"))
      }
    }
  }

  test("Imputer read/write") {
    val t = new Imputer()
      .setInputCols(Array("myInputCol"))
      .setOutputCols(Array("myOutputCol"))
      .setMissingValue(-1.0)
    testDefaultReadWrite(t)
  }

  test("ImputerModel read/write") {
    val spark = this.spark
    import spark.implicits._
    val surrogateDF = Seq(1.234).toDF("myInputCol")

    val instance = new ImputerModel(
      "myImputer", surrogateDF)
      .setInputCols(Array("myInputCol"))
      .setOutputCols(Array("myOutputCol"))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.surrogateDF.columns === instance.surrogateDF.columns)
    assert(newInstance.surrogateDF.collect() === instance.surrogateDF.collect())
  }

}

object ImputerSuite {

  /**
   * Imputation strategy. Available options are ["mean", "median"].
   * @param df DataFrame with columns "id", "value", "expected_mean", "expected_median"
   */
  def iterateStrategyTest(imputer: Imputer, df: DataFrame): Unit = {
    val inputCols = imputer.getInputCols

    Seq("mean", "median").foreach { strategy =>
      imputer.setStrategy(strategy)
      val model = imputer.fit(df)
      val resultDF = model.transform(df)
      imputer.getInputCols.zip(imputer.getOutputCols).foreach { case (inputCol, outputCol) =>
        resultDF.select(s"expected_${strategy}_$inputCol", outputCol).collect().foreach {
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
}
