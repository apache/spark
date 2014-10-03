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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types.UserDefinedType
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

class UserTypeSuite extends QueryTest {

  class DenseVector(val data: Array[Double])

  class VectorRowSerializer extends UserDefinedType[DenseVector] {

    override def dataType: StructType =
      StructType(Seq(StructField("features", ArrayType(DoubleType), nullable = false)))

    override def serialize(obj: DenseVector): Row = Row(obj.data)

    override def deserialize(row: Row): DenseVector = {
      val arr = new Array[Double](row.length)
      var i = 0
      while (i < row.length) {
        arr(i) = row.getDouble(i)
        i += 1
      }
      new DenseVector(arr)
    }
  }

  case class LabeledPoint(label: Double, features: DenseVector)

  test("register user type: LabeledPoint") {
    TestSQLContext.registerUserType(classOf[DenseVector], classOf[VectorRowSerializer])

    val points = Seq(
      LabeledPoint(1.0, new DenseVector(Array(1.0, 0.0))),
      LabeledPoint(0.0, new DenseVector(Array(1.0, -1.0))))
    val pointsRDD: RDD[LabeledPoint] = sparkContext.parallelize(points)

    val features: RDD[DenseVector] =
      pointsRDD.select('features).map { case Row(v: DenseVector) => v }
    val featuresArrays: Array[DenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new DenseVector(Array(1.0, 0.0))))
    assert(featuresArrays.contains(new DenseVector(Array(1.0, -1.0))))

    val labels: RDD[Double] = pointsRDD.select('labels).map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))
  }

}
