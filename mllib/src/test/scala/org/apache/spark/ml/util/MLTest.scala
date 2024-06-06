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

package org.apache.spark.ml.util

import java.io.File

import org.scalatest.Suite

import org.apache.spark.{DebugFilesystem, SparkConf, SparkContext, TestUtils}
import org.apache.spark.internal.config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK
import org.apache.spark.ml.{Model, PredictionModel, Transformer}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

trait MLTest extends StreamTest with TempDirectory { self: Suite =>

  @transient var sc: SparkContext = _
  @transient var checkpointDir: String = _

  protected override def sparkConf = {
    new SparkConf()
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
  }

  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[2]", "MLlibUnitTest", sparkConf))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = spark.sparkContext
    checkpointDir = Utils.createDirectory(tempDir.getCanonicalPath, "checkpoints").toString
    sc.setCheckpointDir(checkpointDir)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(new File(checkpointDir))
    } finally {
      super.afterAll()
    }
  }

  private[ml] def checkVectorSizeOnDF(
      dataframe: DataFrame,
      vecColName: String,
      vecSize: Int): Unit = {
    import dataframe.sparkSession.implicits._
    val group = AttributeGroup.fromStructField(dataframe.schema(vecColName))
    assert(group.size === vecSize,
      s"the vector size obtained from schema should be $vecSize, but got ${group.size}")
    val sizeUDF = udf { vector: Vector => vector.size }
    assert(dataframe.select(sizeUDF(col(vecColName)))
      .as[Int]
      .collect()
      .forall(_ === vecSize))
  }

  private[ml] def checkNominalOnDF(
      dataframe: DataFrame,
      colName: String,
      numValues: Int): Unit = {
    import dataframe.sparkSession.implicits._
    val n = Attribute.fromStructField(dataframe.schema(colName)) match {
      case binAttr: BinaryAttribute => Some(2)
      case nomAttr: NominalAttribute => nomAttr.getNumValues
      case unknown =>
        throw new IllegalArgumentException(s"Attribute type: ${unknown.getClass.getName}")
    }
    assert(n.isDefined && n.get === numValues,
      s"the number of values obtained from schema should be $numValues, but got $n")
    assert(dataframe.select(colName)
      .as[Double]
      .collect()
      .forall(v => v === v.toInt && v >= 0 && v < numValues))
  }

  private[util] def testTransformerOnStreamData[A : Encoder](
      dataframe: DataFrame,
      transformer: Transformer,
      firstResultCol: String,
      otherResultCols: String*)
      (globalCheckFunction: Seq[Row] => Unit): Unit = {

    val columnNames = dataframe.schema.fieldNames
    val stream = MemoryStream[A]
    val columnsWithMetadata = dataframe.schema.map { structField =>
      col(structField.name).as(structField.name, structField.metadata)
    }
    import org.apache.spark.util.ArrayImplicits._
    val streamDF = stream.toDS()
      .toDF(columnNames.toImmutableArraySeq: _*).select(columnsWithMetadata: _*)
    val data = dataframe.as[A].collect()

    val streamOutput = transformer.transform(streamDF)
      .select(firstResultCol, otherResultCols: _*)
    testStream(streamOutput) (
      AddData(stream, data.toImmutableArraySeq: _*),
      CheckAnswer(globalCheckFunction)
    )
  }

  private[util] def testTransformerOnDF(
      dataframe: DataFrame,
      transformer: Transformer,
      firstResultCol: String,
      otherResultCols: String*)
      (globalCheckFunction: Seq[Row] => Unit): Unit = {
    val dfOutput = transformer.transform(dataframe)
    val outputs = dfOutput.select(firstResultCol, otherResultCols: _*).collect()
    globalCheckFunction(outputs.toImmutableArraySeq)
  }

  def testTransformer[A : Encoder](
      dataframe: DataFrame,
      transformer: Transformer,
      firstResultCol: String,
      otherResultCols: String*)
      (checkFunction: Row => Unit): Unit = {
    testTransformerByGlobalCheckFunc(
      dataframe,
      transformer,
      firstResultCol,
      otherResultCols: _*) { rows: Seq[Row] => rows.foreach(checkFunction(_)) }
  }

  def testTransformerByGlobalCheckFunc[A : Encoder](
      dataframe: DataFrame,
      transformer: Transformer,
      firstResultCol: String,
      otherResultCols: String*)
      (globalCheckFunction: Seq[Row] => Unit): Unit = {
    testTransformerOnStreamData(dataframe, transformer, firstResultCol,
      otherResultCols: _*)(globalCheckFunction)
    testTransformerOnDF(dataframe, transformer, firstResultCol,
      otherResultCols: _*)(globalCheckFunction)
    }

  def testTransformerByInterceptingException[A : Encoder](
    dataframe: DataFrame,
    transformer: Transformer,
    expectedMessagePart : String,
    firstResultCol: String): Unit = {

    withClue(s"""Expected message part "${expectedMessagePart}" is not found in DF test.""") {
      val exceptionOnDf = intercept[Throwable] {
        testTransformerOnDF(dataframe, transformer, firstResultCol)(_ => ())
      }
      TestUtils.assertExceptionMsg(exceptionOnDf, expectedMessagePart)
    }
    withClue(s"""Expected message part "${expectedMessagePart}" is not found in stream test.""") {
      val exceptionOnStreamData = intercept[Throwable] {
        testTransformerOnStreamData(dataframe, transformer, firstResultCol)(_ => ())
      }
      TestUtils.assertExceptionMsg(exceptionOnStreamData, expectedMessagePart)
    }
  }

  def testPredictionModelSinglePrediction(model: PredictionModel[Vector, _],
    dataset: Dataset[_]): Unit = {
    model.transform(dataset).select(model.getFeaturesCol, model.getPredictionCol)
      .collect().foreach {
      case Row(features: Vector, prediction: Double) =>
        assert(prediction === model.predict(features))
    }
  }

  def testClassificationModelSingleRawPrediction(model: ClassificationModel[Vector, _],
    dataset: Dataset[_]): Unit = {
    model.transform(dataset).select(model.getFeaturesCol, model.getRawPredictionCol)
      .collect().foreach {
      case Row(features: Vector, rawPrediction: Vector) =>
        assert(rawPrediction === model.predictRaw(features))
    }
  }

  def testProbClassificationModelSingleProbPrediction(
    model: ProbabilisticClassificationModel[Vector, _],
    dataset: Dataset[_]): Unit = {
    model.transform(dataset).select(model.getFeaturesCol, model.getProbabilityCol)
      .collect().foreach {
      case Row(features: Vector, probPrediction: Vector) =>
        assert(probPrediction === model.predictProbability(features))
    }
  }

  def testClusteringModelSinglePrediction(
    model: Model[_],
    transform: Vector => Int,
    dataset: Dataset[_],
    input: String,
    output: String): Unit = {
    model.transform(dataset).select(input, output)
      .collect().foreach {
      case Row(features: Vector, prediction: Int) =>
        assert(prediction === transform(features))
    }
  }

  def testClusteringModelSingleProbabilisticPrediction(
    model: Model[_],
    transform: Vector => Vector,
    dataset: Dataset[_],
    input: String,
    output: String): Unit = {
    model.transform(dataset).select(input, output)
      .collect().foreach {
      case Row(features: Vector, prediction: Vector) =>
        assert(prediction === transform(features))
    }
  }

  def testInvalidRegressionLabels(f: DataFrame => Any): Unit = {
    import testImplicits._

    // labels contains NULL
    val df1 = sc.parallelize(Seq(
      (null, 1.0, Vectors.dense(1.0, 2.0)),
      ("1.0", 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("str_label", "weight", "features")
      .select(col("str_label").cast("double").as("label"), col("weight"), col("features"))
    val e1 = intercept[Exception](f(df1))
    assert(e1.getMessage.contains("Labels MUST NOT be Null or NaN"))

    // labels contains NaN
    val df2 = sc.parallelize(Seq(
      (Double.NaN, 1.0, Vectors.dense(1.0, 2.0)),
      (1.0, 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e2 = intercept[Exception](f(df2))
    assert(e2.getMessage.contains("Labels MUST NOT be Null or NaN"))

    // labels contains invalid value: Infinity
    val df3 = sc.parallelize(Seq(
      (Double.NegativeInfinity, 1.0, Vectors.dense(1.0, 2.0)),
      (1.0, 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e3 = intercept[Exception](f(df3))
    assert(e3.getMessage.contains("Labels MUST NOT be Infinity"))
  }

  def testInvalidClassificationLabels(f: DataFrame => Any, numClasses: Option[Int]): Unit = {
    import testImplicits._

    // labels contains NULL
    val df1 = sc.parallelize(Seq(
      (null, 1.0, Vectors.dense(1.0, 2.0)),
      ("1.0", 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("str_label", "weight", "features")
      .select(col("str_label").cast("double").as("label"), col("weight"), col("features"))
    val e1 = intercept[Exception](f(df1))
    assert(e1.getMessage.contains("Labels MUST NOT be Null or NaN"))

    // labels contains NaN
    val df2 = sc.parallelize(Seq(
      (Double.NaN, 1.0, Vectors.dense(1.0, 2.0)),
      (1.0, 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e2 = intercept[Exception](f(df2))
    assert(e2.getMessage.contains("Labels MUST NOT be Null or NaN"))

    numClasses match {
      case Some(2) =>
        // labels contains invalid value: 3
        val df3 = sc.parallelize(Seq(
          (3.0, 1.0, Vectors.dense(1.0, 2.0)),
          (1.0, 1.0, Vectors.dense(1.0, 2.0))
        )).toDF("label", "weight", "features")
        val e3 = intercept[Exception](f(df3))
        assert(e3.getMessage.contains("Labels MUST be in {0, 1}"))

      case _ =>
        // labels contains invalid value: -3
        val df3 = sc.parallelize(Seq(
          (1.0, 1.0, Vectors.dense(1.0, 2.0)),
          (-3.0, 1.0, Vectors.dense(1.0, 2.0))
        )).toDF("label", "weight", "features")
        val e3 = intercept[Exception](f(df3))
        assert(e3.getMessage.contains("Labels MUST be in [0"))

        // labels contains invalid value: Infinity
        val df4 = sc.parallelize(Seq(
          (1.0, 1.0, Vectors.dense(1.0, 2.0)),
          (Double.PositiveInfinity, 1.0, Vectors.dense(1.0, 2.0))
        )).toDF("label", "weight", "features")
        val e4 = intercept[Exception](f(df4))
        assert(e4.getMessage.contains("Labels MUST be in [0"))

        // labels contains invalid value: 0.1
        val df5 = sc.parallelize(Seq(
          (1.0, 1.0, Vectors.dense(1.0, 2.0)),
          (0.1, 1.0, Vectors.dense(1.0, 2.0))
        )).toDF("label", "weight", "features")
        val e5 = intercept[Exception](f(df5))
        assert(e5.getMessage.contains("Labels MUST be Integers"))
    }
  }

  def testInvalidWeights(f: DataFrame => Any): Unit = {
    import testImplicits._

    // weights contains NULL
    val df1 = sc.parallelize(Seq(
      (1.0, "1.0", Vectors.dense(1.0, 2.0)),
      (0.0, null, Vectors.dense(1.0, 2.0))
    )).toDF("label", "str_weight", "features")
      .select(col("label"), col("str_weight").cast("double").as("weight"), col("features"))
    val e1 = intercept[Exception](f(df1))
    assert(e1.getMessage.contains("Weights MUST NOT be Null or NaN"))

    // weights contains NaN
    val df2 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, Double.NaN, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e2 = intercept[Exception](f(df2))
    assert(e2.getMessage.contains("Weights MUST NOT be Null or NaN"))

    // weights contains invalid value: -3
    val df3 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, -3.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e3 = intercept[Exception](f(df3))
    assert(e3.getMessage.contains("Weights MUST NOT be Negative or Infinity"))

    // weights contains invalid value: Infinity
    val df4 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, Double.PositiveInfinity, Vectors.dense(1.0, 2.0))
    )).toDF("label", "weight", "features")
    val e4 = intercept[Exception](f(df4))
    assert(e4.getMessage.contains("Weights MUST NOT be Negative or Infinity"))
  }

  def testInvalidVectors(f: DataFrame => Any): Unit = {
    import testImplicits._

    // vectors contains NULL
    val df1 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, 1.0, null)
    )).toDF("label", "weight", "features")
    val e1 = intercept[Exception](f(df1))
    assert(e1.getMessage.contains("Vectors MUST NOT be Null"))

    // vectors contains NaN
    val df2 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, 1.0, Vectors.dense(Double.NaN, 2.0))
    )).toDF("label", "weight", "features")
    val e2 = intercept[Exception](f(df2))
    assert(e2.getMessage.contains("Vector values MUST NOT be NaN or Infinity"))

    // vectors contains Infinity
    val df3 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, 1.0, Vectors.dense(1.0, Double.NegativeInfinity))
    )).toDF("label", "weight", "features")
    val e3 = intercept[Exception](f(df3))
    assert(e3.getMessage.contains("Vector values MUST NOT be NaN or Infinity"))
  }
}
