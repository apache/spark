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

import scala.beans.{BeanInfo, BeanProperty}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dsl._
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
private[sql] class MyDenseVector(val data: Array[Double]) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

@BeanInfo
private[sql] case class MyLabeledPoint(
    @BeanProperty label: Double,
    @BeanProperty features: MyDenseVector)

private[sql] class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: Any): Seq[Double] = {
    obj match {
      case features: MyDenseVector =>
        features.data.toSeq
    }
  }

  override def deserialize(datum: Any): MyDenseVector = {
    datum match {
      case data: Seq[_] =>
        new MyDenseVector(data.asInstanceOf[Seq[Double]].toArray)
    }
  }

  override def userClass = classOf[MyDenseVector]
}

class UserDefinedTypeSuite extends QueryTest {
  val points = Seq(
    MyLabeledPoint(1.0, new MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new MyDenseVector(Array(0.2, 2.0))))
  val pointsRDD: RDD[MyLabeledPoint] = sparkContext.parallelize(points)


  test("register user type: MyDenseVector for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDenseVector] =
      pointsRDD.select('features).rdd.map { case Row(v: MyDenseVector) => v }
    val featuresArrays: Array[MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    udf.register("testType", (d: MyDenseVector) => d.isInstanceOf[MyDenseVector])
    pointsRDD.registerTempTable("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }
}
