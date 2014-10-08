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
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.UserDefinedType
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

class DenseVector(val data: Array[Double]) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: DenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

case class LabeledPoint(label: Double, features: DenseVector)

class UserDefinedTypeSuite extends QueryTest {

  object LabeledPointUDT {
    def dataType: StructType =
      StructType(Seq(
        StructField("label", DoubleType, nullable = false),
        StructField("features", ArrayType(DoubleType, containsNull = false), nullable = false)))
  }

  case class LabeledPointUDT() extends UserDefinedType[LabeledPoint](LabeledPointUDT.dataType) {

    override def serialize(obj: Any): Row = obj match {
      case lp: LabeledPoint =>
        val row: GenericMutableRow = new GenericMutableRow(1 + lp.features.data.length)
        row.setDouble(0, lp.label)
        var i = 0
        while (i < lp.features.data.length) {
          row.setDouble(1 + i, lp.features.data(i))
          i += 1
        }
        row
    }

    override def deserialize(row: Row): LabeledPoint = {
      assert(row.length >= 1)
      val label = row.getDouble(0)
      val numFeatures = row.length - 1
      val features = new DenseVector(new Array[Double](numFeatures))
      var i = 0
      while (i < numFeatures) {
        features.data(i) = row.getDouble(1 + i)
        i += 1
      }
      LabeledPoint(label, features)
    }
  }

  test("register user type: LabeledPoint") {
    TestSQLContext.registerUserType(new LabeledPointUDT())
    println("udtRegistry:")
    TestSQLContext.udtRegistry.foreach { case (t, s) => println(s"$t -> $s")}

    println(s"test: ${scala.reflect.runtime.universe.typeTag[LabeledPoint]}")
    assert(TestSQLContext.udtRegistry.contains(scala.reflect.runtime.universe.typeTag[LabeledPoint]))

    val points = Seq(
      LabeledPoint(1.0, new DenseVector(Array(0.1, 1.0))),
      LabeledPoint(0.0, new DenseVector(Array(0.2, 2.0))))
    val pointsRDD: RDD[LabeledPoint] = sparkContext.parallelize(points)

    println("Converting to SchemaRDD")
    val tmpSchemaRDD: SchemaRDD = TestSQLContext.createSchemaRDD(pointsRDD)
    println("blah")
    println(s"SchemaRDD count: ${tmpSchemaRDD.count()}")
    println("Done converting to SchemaRDD")

    // TODO: This test works even when the deserialize method is never used.  How can I test deserialize?
    val features: RDD[DenseVector] =
      pointsRDD.select('features).map { case Row(v: DenseVector) => v}
    val featuresArrays: Array[DenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new DenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new DenseVector(Array(0.2, 2.0))))

    val labels: RDD[Double] = pointsRDD.select('label).map { case Row(v: Double) => v}
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))
  }

  test("UDTs cannot be registered twice") {
    // TODO
  }

  test("UDTs cannot override built-in types") {
    // TODO
  }

}
