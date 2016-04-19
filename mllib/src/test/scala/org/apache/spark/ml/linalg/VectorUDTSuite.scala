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

package org.apache.spark.ml.linalg.udt

import scala.beans.{BeanInfo, BeanProperty}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

@BeanInfo
private[ml] case class MyVectorPoint(
    @BeanProperty label: Double,
    @BeanProperty vector: Vector)

class VectorUDTSuite extends SparkFunSuite with MLlibTestSparkContext {
  import testImplicits._

  test("preloaded VectorUDT") {
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0, 2.0)
    val sv0 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(2, Array(1), Array(2.0))

    val vectorRDD = Seq(
      MyVectorPoint(1.0, dv0),
      MyVectorPoint(2.0, dv1),
      MyVectorPoint(3.0, sv0),
      MyVectorPoint(4.0, sv1)).toDF()

    val labels: RDD[Double] = vectorRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 4)
    assert(labelsArrays.sorted === Array(1.0, 2.0, 3.0, 4.0))

    val vectors: RDD[Vector] =
      vectorRDD.select('vector).rdd.map { case Row(v: Vector) => v }
    val vectorsArrays: Array[Vector] = vectors.collect()
    assert(vectorsArrays.contains(dv0))
    assert(vectorsArrays.contains(dv1))
    assert(vectorsArrays.contains(sv0))
    assert(vectorsArrays.contains(sv1))
  }
}
