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
import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.UserDefinedType
import org.apache.spark.sql.test.TestSQLContext._

@SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
class MyDenseVector(val data: Array[Double]) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

case class MyLabeledPoint(label: Double, features: MyDenseVector)

class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

  override def sqlType: ArrayType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: Any): Row = obj match {
    case features: MyDenseVector =>
      val row: GenericMutableRow = new GenericMutableRow(features.data.length)
      var i = 0
      while (i < features.data.length) {
        row.setDouble(i, features.data(i))
        i += 1
      }
      row
  }

  override def deserialize(row: Row): MyDenseVector = {
    val features = new MyDenseVector(new Array[Double](row.length))
    var i = 0
    while (i < row.length) {
      features.data(i) = row.getDouble(i)
      i += 1
    }
    features
  }
}

object UserDefinedTypeSuiteObject {

  class ClassInObject(val dv: MyDenseVector) extends Serializable

  case class MyLabeledPointInObject(label: Double, features: ClassInObject)

  class ClassInObjectUDT extends UserDefinedType[ClassInObject] {

    override def sqlType: ArrayType = ArrayType(DoubleType, containsNull = false)

    private val dvUDT = new MyDenseVectorUDT()

    override def serialize(obj: Any): Row = obj match {
      case cio: ClassInObject => dvUDT.serialize(cio)
    }

    override def deserialize(row: Row): ClassInObject = new ClassInObject(dvUDT.deserialize(row))
  }
}

class UserDefinedTypeSuite extends QueryTest {

  test("register user type: MyDenseVector for MyLabeledPoint") {
    val points = Seq(
      MyLabeledPoint(1.0, new MyDenseVector(Array(0.1, 1.0))),
      MyLabeledPoint(0.0, new MyDenseVector(Array(0.2, 2.0))))
    val pointsRDD: RDD[MyLabeledPoint] = sparkContext.parallelize(points)

    val labels: RDD[Double] = pointsRDD.select('label).map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDenseVector] =
      pointsRDD.select('features).map { case Row(v: MyDenseVector) => v }
    val featuresArrays: Array[MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector(Array(0.2, 2.0))))
  }

}
