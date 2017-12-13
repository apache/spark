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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

class VectorSizeHintSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("Test Param Validators") {
    intercept[IllegalArgumentException] (new VectorSizeHint().setHandleInvalid("invalidValue"))
    intercept[IllegalArgumentException] (new VectorSizeHint().setSize(-3))
  }

  test("Required params must be set before transform.") {
    val data = Seq((Vectors.dense(1, 2), 0)).toDF("vector", "intValue")

    val noSizeTransformer = new VectorSizeHint().setInputCol("vector")
    intercept[NoSuchElementException] (noSizeTransformer.transform(data))
    intercept[NoSuchElementException] (noSizeTransformer.transformSchema(data.schema))

    val noInputColTransformer = new VectorSizeHint().setSize(2)
    intercept[NoSuchElementException] (noInputColTransformer.transform(data))
    intercept[NoSuchElementException] (noInputColTransformer.transformSchema(data.schema))
  }

  test("Adding size to column of vectors.") {

    val size = 3
    val vectorColName = "vector"
    val denseVector = Vectors.dense(1, 2, 3)
    val sparseVector = Vectors.sparse(size, Array(), Array())

    val data = Seq(denseVector, denseVector, sparseVector).map(Tuple1.apply)
    val dataFrame = data.toDF(vectorColName)
    assert(
      AttributeGroup.fromStructField(dataFrame.schema(vectorColName)).size == -1,
      s"This test requires that column '$vectorColName' not have size metadata.")

    for (handleInvalid <- VectorSizeHint.supportedHandleInvalids) {
      val transformer = new VectorSizeHint()
        .setInputCol(vectorColName)
        .setSize(size)
        .setHandleInvalid(handleInvalid)
      val withSize = transformer.transform(dataFrame)
      assert(
        AttributeGroup.fromStructField(withSize.schema(vectorColName)).size == size,
        "Transformer did not add expected size data.")
      val numRows = withSize.collect().length
      assert(numRows === data.length, s"Expecting ${data.length} rows, got $numRows.")
    }
  }

  test("Size hint preserves attributes.") {

    val size = 3
    val vectorColName = "vector"
    val data = Seq((1, 2, 3), (2, 3, 3))
    val dataFrame = data.toDF("x", "y", "z")

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z"))
      .setOutputCol(vectorColName)
    val dataFrameWithMetadata = assembler.transform(dataFrame)
    val group = AttributeGroup.fromStructField(dataFrameWithMetadata.schema(vectorColName))

    for (handleInvalid <- VectorSizeHint.supportedHandleInvalids) {
      val transformer = new VectorSizeHint()
        .setInputCol(vectorColName)
        .setSize(size)
        .setHandleInvalid(handleInvalid)
      val withSize = transformer.transform(dataFrameWithMetadata)

      val newGroup = AttributeGroup.fromStructField(withSize.schema(vectorColName))
      assert(newGroup.size === size, "Column has incorrect size metadata.")
      assert(
        newGroup.attributes.get === group.attributes.get,
        "VectorSizeHint did not preserve attributes.")
      withSize.collect
    }
  }

  test("Size mismatch between current and target size raises an error.") {
    val size = 4
    val vectorColName = "vector"
    val data = Seq((1, 2, 3), (2, 3, 3))
    val dataFrame = data.toDF("x", "y", "z")

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z"))
      .setOutputCol(vectorColName)
    val dataFrameWithMetadata = assembler.transform(dataFrame)

    for (handleInvalid <- VectorSizeHint.supportedHandleInvalids) {
      val transformer = new VectorSizeHint()
        .setInputCol(vectorColName)
        .setSize(size)
        .setHandleInvalid(handleInvalid)
      intercept[IllegalArgumentException](transformer.transform(dataFrameWithMetadata))
    }
  }

  test("Handle invalid does the right thing.") {

    val vector = Vectors.dense(1, 2, 3)
    val short = Vectors.dense(2)
    val dataWithNull = Seq(vector, null).map(Tuple1.apply).toDF("vector")
    val dataWithShort = Seq(vector, short).map(Tuple1.apply).toDF("vector")

    val sizeHint = new VectorSizeHint()
      .setInputCol("vector")
      .setHandleInvalid("error")
      .setSize(3)

    intercept[SparkException](sizeHint.transform(dataWithNull).collect())
    intercept[SparkException](sizeHint.transform(dataWithShort).collect())

    sizeHint.setHandleInvalid("skip")
    assert(sizeHint.transform(dataWithNull).count() === 1)
    assert(sizeHint.transform(dataWithShort).count() === 1)
  }

  test("read/write") {
    val sizeHint = new VectorSizeHint()
      .setInputCol("myInputCol")
      .setSize(11)
      .setHandleInvalid("skip")
    testDefaultReadWrite(sizeHint)
  }
}

class VectorSizeHintStreamingSuite extends StreamTest {

  import testImplicits._

  test("Test assemble vectors with size hint in streaming.") {
    val a = Vectors.dense(0, 1, 2)
    val b = Vectors.sparse(4, Array(0, 3), Array(3, 6))

    val stream = MemoryStream[(Vector, Vector)]
    val streamingDF = stream.toDS.toDF("a", "b")
    val sizeHintA = new VectorSizeHint()
      .setSize(3)
      .setInputCol("a")
    val sizeHintB = new VectorSizeHint()
      .setSize(4)
      .setInputCol("b")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b"))
      .setOutputCol("assembled")
    val pipeline = new Pipeline().setStages(Array(sizeHintA, sizeHintB, vectorAssembler))
    /**
    val output = Seq(sizeHintA, sizeHintB, vectorAssembler).foldLeft(streamingDF) {
      case (data, transformer) => transformer.transform(data)
    }.select("assembled")
    */
    val output = pipeline.fit(streamingDF).transform(streamingDF).select("assembled")

    val expected = Vectors.dense(0, 1, 2, 3, 0, 0, 6)

    testStream (output) (
      AddData(stream, (a, b), (a, b)),
      CheckAnswer(Tuple1(expected), Tuple1(expected))
    )
  }
}
