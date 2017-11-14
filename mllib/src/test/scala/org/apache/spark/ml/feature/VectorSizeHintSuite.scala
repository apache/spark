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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

class VectorSizeHintSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("Test Param Validators") {
    assertThrows[IllegalArgumentException] (new VectorSizeHint().setHandleInvalid("invalidValue"))
    assertThrows[IllegalArgumentException] (new VectorSizeHint().setSize(-3))
  }

  test("Adding size to column of vectors.") {

    val size = 3
    val denseVector = Vectors.dense(1, 2, 3)
    val sparseVector = Vectors.sparse(size, Array(), Array())

    val data = Seq(denseVector, denseVector, sparseVector).map(Tuple1.apply)
    val dataFrame = data.toDF("vector")

    val transformer = new VectorSizeHint()
      .setInputCol("vector")
      .setSize(3)
      .setHandleInvalid("error")
    val withSize = transformer.transform(dataFrame)
    assert(
      AttributeGroup.fromStructField(withSize.schema("vector")).size == size,
      "Transformer did not add expected size data.")
  }

  test("Size hint preserves attributes.") {

    case class Foo(x: Double, y: Double, z: Double)
    val size = 3
    val data = Seq((1, 2, 3), (2, 3, 3))
    val boo = data.toDF("x", "y", "z")

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z"))
      .setOutputCol("vector")
    val dataFrameWithMeatadata = assembler.transform(boo)
    val group = AttributeGroup.fromStructField(dataFrameWithMeatadata.schema("vector"))

    val transformer = new VectorSizeHint()
      .setInputCol("vector")
      .setSize(3)
      .setHandleInvalid("error")
    val withSize = transformer.transform(dataFrameWithMeatadata)

    val newGroup = AttributeGroup.fromStructField(withSize.schema("vector"))
    assert(newGroup.size == size, "Transformer did not add expected size data.")
    assert(
      newGroup.attributes.get.deep === group.attributes.get.deep,
      "SizeHintTransformer did not preserve attributes.")
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

    assertThrows[SparkException](sizeHint.transform(dataWithNull).collect)
    assertThrows[SparkException](sizeHint.transform(dataWithShort).collect)

    sizeHint.setHandleInvalid("skip")
    assert(sizeHint.transform(dataWithNull).count() === 1)
    assert(sizeHint.transform(dataWithShort).count() === 1)
  }
}

class VectorSizeHintStreamingSuite extends StreamTest {

  import testImplicits._

  test("Test assemble vectors with size hint in steaming.") {
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
    val output = Seq(sizeHintA, sizeHintB, vectorAssembler).foldLeft(streamingDF) {
      case (data, transform) => transform.transform(data)
    }.select("assembled")

    val expected = Vectors.dense(0, 1, 2, 3, 0, 0, 6)

    testStream (output) (
      AddData(stream, (a, b), (a, b)),
      CheckAnswerRows(Seq(Row(expected), Row(expected)), false, false)
    )
  }
}
