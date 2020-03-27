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
import org.apache.spark.ml.classification.{ClassificationModel, ProbabilisticClassificationModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.TestSparkSession
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
    val streamDF = stream.toDS().toDF(columnNames: _*).select(columnsWithMetadata: _*)
    val data = dataframe.as[A].collect()

    val streamOutput = transformer.transform(streamDF)
      .select(firstResultCol, otherResultCols: _*)
    testStream(streamOutput) (
      AddData(stream, data: _*),
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
    globalCheckFunction(outputs)
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
}
