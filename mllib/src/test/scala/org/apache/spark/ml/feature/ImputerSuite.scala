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

import org.apache.spark.SparkException
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, SchemaUtils}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ImputerSuite extends MLTest with DefaultReadWriteTest {

  test("Imputer for Double with default missing Value NaN") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 4.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0),
      (1, 11.0, 12.0, 11.0, 11.0, 11.0, 12.0, 12.0, 12.0),
      (2, 3.0, Double.NaN, 3.0, 3.0, 3.0, 10.0, 12.0, 4.0),
      (3, Double.NaN, 14.0, 5.0, 3.0, 1.0, 14.0, 14.0, 14.0)
    )).toDF("id", "value1", "value2",
      "expected_mean_value1", "expected_median_value1", "expected_mode_value1",
      "expected_mean_value2", "expected_median_value2", "expected_mode_value2")
    val imputer = new Imputer()
      .setInputCols(Array("value1", "value2"))
      .setOutputCols(Array("out1", "out2"))
    ImputerSuite.iterateStrategyTest(true, imputer, df)
  }

  test("Single Column: Imputer for Double with default missing Value NaN") {
    val df1 = spark.createDataFrame(Seq(
      (0, 1.0, 1.0, 1.0, 1.0),
      (1, 11.0, 11.0, 11.0, 11.0),
      (2, 3.0, 3.0, 3.0, 3.0),
      (3, Double.NaN, 5.0, 3.0, 1.0)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer1 = new Imputer()
      .setInputCol("value")
      .setOutputCol("out")
    ImputerSuite.iterateStrategyTest(false, imputer1, df1)

    val df2 = spark.createDataFrame(Seq(
      (0, 4.0, 4.0, 4.0, 4.0),
      (1, 12.0, 12.0, 12.0, 12.0),
      (2, Double.NaN, 10.0, 12.0, 4.0),
      (3, 14.0, 14.0, 14.0, 14.0)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer2 = new Imputer()
      .setInputCol("value")
      .setOutputCol("out")
    ImputerSuite.iterateStrategyTest(false, imputer2, df2)
  }

  test("Imputer should handle NaNs when computing surrogate value, if missingValue is not NaN") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 1.0, 1.0, 1.0),
      (1, 3.0, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN, Double.NaN),
      (3, -1.0, 2.0, 1.0, 1.0)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
      .setMissingValue(-1.0)
    ImputerSuite.iterateStrategyTest(true, imputer, df)
  }

  test("Single Column: Imputer should handle NaNs when computing surrogate value," +
    " if missingValue is not NaN") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 1.0, 1.0, 1.0),
      (1, 3.0, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN, Double.NaN),
      (3, -1.0, 2.0, 1.0, 1.0)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
      .setMissingValue(-1.0)
    ImputerSuite.iterateStrategyTest(false, imputer, df)
  }

  test("Imputer for Float with missing Value -1.0") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0F, 1.0F, 1.0F, 1.0F),
      (1, 3.0F, 3.0F, 3.0F, 3.0F),
      (2, 10.0F, 10.0F, 10.0F, 10.0F),
      (3, 10.0F, 10.0F, 10.0F, 10.0F),
      (4, -1.0F, 6.0F, 3.0F, 10.0F)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
      .setMissingValue(-1)
    ImputerSuite.iterateStrategyTest(true, imputer, df)
  }

  test("Single Column: Imputer for Float with missing Value -1.0") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0F, 1.0F, 1.0F, 1.0F),
      (1, 3.0F, 3.0F, 3.0F, 3.0F),
      (2, 10.0F, 10.0F, 10.0F, 10.0F),
      (3, 10.0F, 10.0F, 10.0F, 10.0F),
      (4, -1.0F, 6.0F, 3.0F, 10.0F)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
      .setMissingValue(-1)
    ImputerSuite.iterateStrategyTest(false, imputer, df)
  }

  test("Imputer should impute null as well as 'missingValue'") {
    val rawDf = spark.createDataFrame(Seq(
      (0, 4.0, 4.0, 4.0, 4.0),
      (1, 10.0, 10.0, 10.0, 10.0),
      (2, 10.0, 10.0, 10.0, 10.0),
      (3, Double.NaN, 8.0, 10.0, 10.0),
      (4, -1.0, 8.0, 10.0, 10.0)
    )).toDF("id", "rawValue",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val df = rawDf.selectExpr("*", "IF(rawValue=-1.0, null, rawValue) as value")
    val imputer = new Imputer().setInputCols(Array("value")).setOutputCols(Array("out"))
    ImputerSuite.iterateStrategyTest(true, imputer, df)
  }

  test("Single Column: Imputer should impute null as well as 'missingValue'") {
    val rawDf = spark.createDataFrame(Seq(
      (0, 4.0, 4.0, 4.0, 4.0),
      (1, 10.0, 10.0, 10.0, 10.0),
      (2, 10.0, 10.0, 10.0, 10.0),
      (3, Double.NaN, 8.0, 10.0, 10.0),
      (4, -1.0, 8.0, 10.0, 10.0)
    )).toDF("id", "rawValue",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    val df = rawDf.selectExpr("*", "IF(rawValue=-1.0, null, rawValue) as value")
    val imputer = new Imputer().setInputCol("value").setOutputCol("out")
    ImputerSuite.iterateStrategyTest(false, imputer, df)
  }

  test("Imputer should work with Structured Streaming") {
    val localSpark = spark
    import localSpark.implicits._
    val df = Seq[(java.lang.Double, Double)](
      (4.0, 4.0),
      (10.0, 10.0),
      (10.0, 10.0),
      (Double.NaN, 8.0),
      (null, 8.0)
    ).toDF("value", "expected_mean_value")
    val imputer = new Imputer()
      .setInputCols(Array("value"))
      .setOutputCols(Array("out"))
      .setStrategy("mean")
    val model = imputer.fit(df)
    testTransformer[(java.lang.Double, Double)](df, model, "expected_mean_value", "out") {
      case Row(exp: java.lang.Double, out: Double) =>
        assert((exp.isNaN && out.isNaN) || (exp == out),
          s"Imputed values differ. Expected: $exp, actual: $out")
    }
  }

  test("Single Column: Imputer should work with Structured Streaming") {
    val localSpark = spark
    import localSpark.implicits._
    val df = Seq[(java.lang.Double, Double)](
      (4.0, 4.0),
      (10.0, 10.0),
      (10.0, 10.0),
      (Double.NaN, 8.0),
      (null, 8.0)
    ).toDF("value", "expected_mean_value")
    val imputer = new Imputer()
      .setInputCol("value")
      .setOutputCol("out")
      .setStrategy("mean")
    val model = imputer.fit(df)
    testTransformer[(java.lang.Double, Double)](df, model, "expected_mean_value", "out") {
      case Row(exp: java.lang.Double, out: Double) =>
        assert((exp.isNaN && out.isNaN) || (exp == out),
          s"Imputed values differ. Expected: $exp, actual: $out")
    }
  }

  test("Imputer throws exception when surrogate cannot be computed") {
    val df = spark.createDataFrame(Seq(
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

  test("Single Column: Imputer throws exception when surrogate cannot be computed") {
    val df = spark.createDataFrame(Seq(
      (0, Double.NaN, 1.0, 1.0, 1.0),
      (1, Double.NaN, 3.0, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    )).toDF("id", "value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")
    Seq("mean", "median", "mode").foreach { strategy =>
      val imputer = new Imputer().setInputCol("value").setOutputCol("out")
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
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 1.0, 1.0),
      (1, Double.NaN, 3.0, 3.0),
      (2, Double.NaN, Double.NaN, Double.NaN)
    )).toDF("id", "value1", "value2", "value3")
    Seq("mean", "median", "mode").foreach { strategy =>
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

      withClue("Imputer should fail if inputCols param is empty.") {
        val e: IllegalArgumentException = intercept[IllegalArgumentException] {
          val imputer = new Imputer().setStrategy(strategy)
            .setInputCols(Array[String]())
            .setOutputCols(Array[String]())
          val model = imputer.fit(df)
        }
        assert(e.getMessage.contains("inputCols cannot be empty"))
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

  test("Single Column: Imputer read/write") {
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
      .setInputCols(Array("myInputCol"))
      .setOutputCols(Array("myOutputCol"))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.surrogateDF.columns === instance.surrogateDF.columns)
    assert(newInstance.surrogateDF.collect() === instance.surrogateDF.collect())
  }

  test("Single Column: ImputerModel read/write") {
    val spark = this.spark
    import spark.implicits._
    val surrogateDF = Seq(1.234).toDF("myInputCol")

    val instance = new ImputerModel(
      "myImputer", surrogateDF)
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.surrogateDF.columns === instance.surrogateDF.columns)
    assert(newInstance.surrogateDF.collect() === instance.surrogateDF.collect())
  }

  test("Imputer for IntegerType with default missing value null") {
    val df = spark.createDataFrame(Seq[(Integer, Integer, Integer, Integer)](
      (1, 1, 1, 1),
      (11, 11, 11, 11),
      (3, 3, 3, 3),
      (null, 5, 3, 1)
    )).toDF("value1",
      "expected_mean_value1", "expected_median_value1", "expected_mode_value1")

    val imputer = new Imputer()
      .setInputCols(Array("value1"))
      .setOutputCols(Array("out1"))

    val types = Seq(IntegerType, LongType)
    import org.apache.spark.util.ArrayImplicits._
    for (mType <- types) {
      // cast all columns to desired data type for testing
      val df2 = df.select(df.columns.map(c => col(c).cast(mType)).toImmutableArraySeq: _*)
      ImputerSuite.iterateStrategyTest(true, imputer, df2)
    }
  }

  test("Single Column Imputer for IntegerType with default missing value null") {
    val df = spark.createDataFrame(Seq[(Integer, Integer, Integer, Integer)](
      (1, 1, 1, 1),
      (11, 11, 11, 11),
      (3, 3, 3, 3),
      (null, 5, 3, 1)
    )).toDF("value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")

    val imputer = new Imputer()
      .setInputCol("value")
      .setOutputCol("out")

    val types = Seq(IntegerType, LongType)
    import org.apache.spark.util.ArrayImplicits._
    for (mType <- types) {
      // cast all columns to desired data type for testing
      val df2 = df.select(df.columns.map(c => col(c).cast(mType)).toImmutableArraySeq: _*)
      ImputerSuite.iterateStrategyTest(false, imputer, df2)
    }
  }

  test("Imputer for IntegerType with missing value -1") {
    val df = spark.createDataFrame(Seq[(Integer, Integer, Integer, Integer)](
      (1, 1, 1, 1),
      (11, 11, 11, 11),
      (3, 3, 3, 3),
      (-1, 5, 3, 1)
    )).toDF("value1",
      "expected_mean_value1", "expected_median_value1", "expected_mode_value1")

    val imputer = new Imputer()
      .setInputCols(Array("value1"))
      .setOutputCols(Array("out1"))
      .setMissingValue(-1.0)

    val types = Seq(IntegerType, LongType)
    import org.apache.spark.util.ArrayImplicits._
    for (mType <- types) {
      // cast all columns to desired data type for testing
      val df2 = df.select(df.columns.map(c => col(c).cast(mType)).toImmutableArraySeq: _*)
      ImputerSuite.iterateStrategyTest(true, imputer, df2)
    }
  }

  test("Single Column: Imputer for IntegerType with missing value -1") {
    val df = spark.createDataFrame(Seq[(Integer, Integer, Integer, Integer)](
      (1, 1, 1, 1),
      (11, 11, 11, 11),
      (3, 3, 3, 3),
      (-1, 5, 3, 1)
    )).toDF("value",
      "expected_mean_value", "expected_median_value", "expected_mode_value")

    val imputer = new Imputer()
      .setInputCol("value")
      .setOutputCol("out")
      .setMissingValue(-1.0)

    val types = Seq(IntegerType, LongType)
    import org.apache.spark.util.ArrayImplicits._
    for (mType <- types) {
      // cast all columns to desired data type for testing
      val df2 = df.select(df.columns.map(c => col(c).cast(mType)).toImmutableArraySeq: _*)
      ImputerSuite.iterateStrategyTest(false, imputer, df2)
    }
  }

  test("assert exception is thrown if both multi-column and single-column params are set") {
    import testImplicits._
    val df = Seq((0.5, 0.3), (0.5, -0.4)).toDF("feature1", "feature2")
    ParamsSuite.testExclusiveParams(new Imputer, df, ("inputCol", "feature1"),
      ("inputCols", Array("feature1", "feature2")))
    ParamsSuite.testExclusiveParams(new Imputer, df, ("inputCol", "feature1"),
      ("outputCol", "result1"), ("outputCols", Array("result1", "result2")))

    // this should fail because at least one of inputCol and inputCols must be set
    ParamsSuite.testExclusiveParams(new Imputer, df, ("outputCol", "feature1"))
  }

  test("Compare single/multiple column(s) Imputer in pipeline") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 4.0),
      (1, 11.0, 12.0),
      (2, 3.0, Double.NaN),
      (3, Double.NaN, 14.0)
    )).toDF("id", "value1", "value2")
    Seq("mean", "median", "mode").foreach { strategy =>
      val multiColsImputer = new Imputer()
        .setInputCols(Array("value1", "value2"))
        .setOutputCols(Array("result1", "result2"))
        .setStrategy(strategy)

      val plForMultiCols = new Pipeline()
        .setStages(Array(multiColsImputer))
        .fit(df)

      val imputerForCol1 = new Imputer()
        .setInputCol("value1")
        .setOutputCol("result1")
        .setStrategy(strategy)
      val imputerForCol2 = new Imputer()
        .setInputCol("value2")
        .setOutputCol("result2")
        .setStrategy(strategy)

      val plForSingleCol = new Pipeline()
        .setStages(Array(imputerForCol1, imputerForCol2))
        .fit(df)

      val resultForSingleCol = plForSingleCol.transform(df)
        .select("result1", "result2")
        .collect()
      val resultForMultiCols = plForMultiCols.transform(df)
        .select("result1", "result2")
        .collect()

      resultForSingleCol.zip(resultForMultiCols).foreach {
        case (rowForSingle, rowForMultiCols) =>
          assert(rowForSingle.getDouble(0) == rowForMultiCols.getDouble(0) &&
            rowForSingle.getDouble(1) == rowForMultiCols.getDouble(1))
      }
    }
  }

  test("Imputer nested input column") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, 4.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0),
      (1, 11.0, 12.0, 11.0, 11.0, 11.0, 12.0, 12.0, 12.0),
      (2, 3.0, Double.NaN, 3.0, 3.0, 3.0, 10.0, 12.0, 4.0),
      (3, Double.NaN, 14.0, 5.0, 3.0, 1.0, 14.0, 14.0, 14.0)
    )).toDF("id", "value1", "value2",
      "expected_mean_value1", "expected_median_value1", "expected_mode_value1",
      "expected_mean_value2", "expected_median_value2", "expected_mode_value2")
      .withColumn("nest", struct("value1", "value2"))
      .drop("value1", "value2")
    val imputer = new Imputer()
      .setInputCols(Array("nest.value1", "nest.value2"))
      .setOutputCols(Array("out1", "out2"))
    ImputerSuite.iterateStrategyTest(true, imputer, df)
  }
}

object ImputerSuite {

  /**
   * Imputation strategy. Available options are ["mean", "median", "mode"].
   * @param df DataFrame with columns "id", "value", "expected_mean", "expected_median",
   *           "expected_mode".
   */
  def iterateStrategyTest(isMultiCol: Boolean, imputer: Imputer, df: DataFrame): Unit = {
    Seq("mean", "median", "mode").foreach { strategy =>
      imputer.setStrategy(strategy)
      val model = imputer.fit(df)
      val resultDF = model.transform(df)
      if (isMultiCol) {
        imputer.getInputCols.zip(imputer.getOutputCols).foreach { case (inputCol, outputCol) =>
          verifyTransformResult(strategy, inputCol, outputCol, resultDF)
        }
      } else {
          verifyTransformResult(strategy, imputer.getInputCol, imputer.getOutputCol, resultDF)
      }
    }
  }

  def verifyTransformResult(
      strategy: String,
      inputCol: String,
      outputCol: String,
      resultDF: DataFrame): Unit = {
    // check dataType is consistent between input and output
    val inputType = SchemaUtils.getSchemaFieldType(resultDF.schema, inputCol)
    val outputType = SchemaUtils.getSchemaFieldType(resultDF.schema, outputCol)
    assert(inputType == outputType, "Output type is not the same as input type.")

    val inputColSplits = inputCol.split("\\.")
    val inputColLastSplit = inputColSplits(inputColSplits.length - 1)
    // check value
    resultDF.select(s"expected_${strategy}_$inputColLastSplit", outputCol).collect().foreach {
      case Row(exp: Float, out: Float) =>
        assert((exp.isNaN && out.isNaN) || (exp == out),
          s"Imputed values differ. Expected: $exp, actual: $out")
      case Row(exp: Double, out: Double) =>
        assert((exp.isNaN && out.isNaN) || (exp ~== out absTol 1e-5),
          s"Imputed values differ. Expected: $exp, actual: $out")
      case Row(exp: Integer, out: Integer) =>
        assert(exp == out,
          s"Imputed values differ. Expected: $exp, actual: $out")
      case Row(exp: Long, out: Long) =>
        assert(exp == out,
          s"Imputed values differ. Expected: $exp, actual: $out")
    }
  }
}
