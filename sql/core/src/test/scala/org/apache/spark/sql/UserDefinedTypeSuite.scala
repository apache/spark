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
import org.apache.spark.sql.catalyst.annotation.UserDefinedType
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.UserDefinedTypeType
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

@UserDefinedType(udt = classOf[DenseVectorUDT])
class DenseVector(val data: Array[Double]) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: DenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

case class LabeledPoint(label: Double, features: DenseVector)

class DenseVectorUDT extends UserDefinedTypeType[DenseVector] {

  override def sqlType: ArrayType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: Any): Row = obj match {
    case features: DenseVector =>
      val row: GenericMutableRow = new GenericMutableRow(features.data.length)
      var i = 0
      while (i < features.data.length) {
        row.setDouble(i, features.data(i))
        i += 1
      }
      row
  }

  override def deserialize(row: Row): DenseVector = {
    val features = new DenseVector(new Array[Double](row.length))
    var i = 0
    while (i < row.length) {
      features.data(i) = row.getDouble(i)
      i += 1
    }
    features
  }
}

class UserDefinedTypeSuite extends QueryTest {

  test("register user type: DenseVector for LabeledPoint") {
    val points = Seq(
      LabeledPoint(1.0, new DenseVector(Array(0.1, 1.0))),
      LabeledPoint(0.0, new DenseVector(Array(0.2, 2.0))))
    val pointsRDD: RDD[LabeledPoint] = sparkContext.parallelize(points)

    val labels: RDD[Double] = pointsRDD.select('label).map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[DenseVector] =
      pointsRDD.select('features).map { case Row(v: DenseVector) => v }
    val featuresArrays: Array[DenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new DenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new DenseVector(Array(0.2, 2.0))))
  }

  /*
    test("UDTs can be registered twice, overriding previous registration") {
    // TODO
  }

  test("UDTs cannot override built-in types") {
    // TODO
  }
  */

}
