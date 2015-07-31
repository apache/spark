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

import org.scalatest.FunSuite

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

class VectorSlicerSuite extends FunSuite with MLlibTestSparkContext {

  test("Test vector slicer") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = Array(
      Vectors.sparse(5, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(-2.0, 2.3, 0.0, 0.0, 1.0),
      Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0, 4.5, 3.3),
      Vectors.sparse(5, Seq())
    )

    val result = Array(
      Vectors.sparse(2, Seq((1, 2.3))),
      Vectors.dense(2.3, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(-1.1, 3.3),
      Vectors.sparse(2, Seq())
    )

    val attrs = Array(
      NumericAttribute.defaultAttr.withIndex(0).withName("first"),
      NumericAttribute.defaultAttr.withIndex(1).withName("second"),
      NumericAttribute.defaultAttr.withIndex(2).withName("third"),
      NumericAttribute.defaultAttr.withIndex(3).withName("fourth"),
      NumericAttribute.defaultAttr.withIndex(4).withName("fifth")
    )

    val resultAttrs = Array(
      NumericAttribute.defaultAttr.withIndex(1).withName("second"),
      NumericAttribute.defaultAttr.withIndex(4).withName("fifth")
    )

    val attrGroup = new AttributeGroup("features", attrs.asInstanceOf[Array[Attribute]])

    val resultAttrGroup = new AttributeGroup("result", resultAttrs.asInstanceOf[Array[Attribute]])

    val rowRDD = sc.parallelize(data.zip(result)).toDF("features", "result").map(row => row)

    val df = sqlContext.createDataFrame(
      rowRDD,
      StructType(Array(attrGroup.toStructField(), resultAttrGroup.toStructField())))

    val vectorSlicer = new VectorSlicer().setInputCol("features")

    vectorSlicer
      .setOutputCol("expected1")
      .setSelectedIndices(Array(1, 4))
      .setSelectedNames(Array("fifth"))
      .transform(df).select("expected1", "result").collect().foreach {
        case Row(vec1: Vector, vec2: Vector) =>
          assert(vec1 ~== vec2 absTol 1e-5)
      }

    vectorSlicer
      .setOutputCol("expected2")
      .setSelectedNames(Array("fifth"))
      .setSelectedIndices(Array(1))
      .transform(df).select("expected2", "result").collect().foreach {
        case Row(vec1: Vector, vec2: Vector) =>
          assert(vec1 ~== vec2 absTol 1e-5)
      }
  }
}

