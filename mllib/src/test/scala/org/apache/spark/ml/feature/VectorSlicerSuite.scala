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

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

class VectorSlicerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    val slicer = new VectorSlicer().setInputCol("feature")
    ParamsSuite.checkParams(slicer)
    assert(slicer.getIndices.length === 0)
    assert(slicer.getNames.length === 0)
    withClue("VectorSlicer should not have any features selected by default") {
      intercept[IllegalArgumentException] {
        slicer.transformSchema(StructType(Seq(StructField("feature", new VectorUDT, true))))
      }
    }
  }

  test("feature validity checks") {
    import VectorSlicer._
    assert(validIndices(Array(0, 1, 8, 2)))
    assert(validIndices(Array.empty[Int]))
    assert(!validIndices(Array(-1)))
    assert(!validIndices(Array(1, 2, 1)))

    assert(validNames(Array("a", "b")))
    assert(validNames(Array.empty[String]))
    assert(!validNames(Array("", "b")))
    assert(!validNames(Array("a", "b", "a")))
  }

  test("Test vector slicer") {
    val data = Array(
      Vectors.sparse(5, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(-2.0, 2.3, 0.0, 0.0, 1.0),
      Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0, 4.5, 3.3),
      Vectors.sparse(5, Seq())
    )

    // Expected after selecting indices 1, 4
    val expected = Array(
      Vectors.sparse(2, Seq((0, 2.3))),
      Vectors.dense(2.3, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(-1.1, 3.3),
      Vectors.sparse(2, Seq())
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f0", "f1", "f2", "f3", "f4").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("features", attrs.asInstanceOf[Array[Attribute]])

    val resultAttrs = Array("f1", "f4").map(defaultAttr.withName)
    val resultAttrGroup = new AttributeGroup("expected", resultAttrs.asInstanceOf[Array[Attribute]])

    val rdd = sc.parallelize(data.zip(expected).toImmutableArraySeq)
      .map { case (a, b) => Row(a, b) }
    val df = spark.createDataFrame(rdd,
      StructType(Array(attrGroup.toStructField(), resultAttrGroup.toStructField())))

    val vectorSlicer = new VectorSlicer().setInputCol("features").setOutputCol("result")

    def validateResults(rows: Seq[Row]): Unit = {
      rows.foreach { case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 === vec2)
      }
      val resultMetadata = AttributeGroup.fromStructField(rows.head.schema("result"))
      val expectedMetadata = AttributeGroup.fromStructField(rows.head.schema("expected"))
      assert(resultMetadata.numAttributes === expectedMetadata.numAttributes)
      resultMetadata.attributes.get.zip(expectedMetadata.attributes.get).foreach { case (a, b) =>
        assert(a === b)
      }
    }

    vectorSlicer.setIndices(Array(1, 4)).setNames(Array.empty)
    testTransformerByGlobalCheckFunc[(Vector, Vector)](df, vectorSlicer, "result", "expected")(
      validateResults)

    vectorSlicer.setIndices(Array(1)).setNames(Array("f4"))
    testTransformerByGlobalCheckFunc[(Vector, Vector)](df, vectorSlicer, "result", "expected")(
      validateResults)

    vectorSlicer.setIndices(Array.empty).setNames(Array("f1", "f4"))
    testTransformerByGlobalCheckFunc[(Vector, Vector)](df, vectorSlicer, "result", "expected")(
      validateResults)
  }

  test("read/write") {
    val t = new VectorSlicer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setIndices(Array(1, 3))
      .setNames(Array("a", "d"))
    testDefaultReadWrite(t)
  }
}
