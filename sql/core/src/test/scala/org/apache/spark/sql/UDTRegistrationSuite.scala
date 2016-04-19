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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

private[sql] trait DVector extends Serializable

private[sql] class MyDVector(val data: Array[Double]) extends DVector with Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

private[sql] class MyDVector2(val data: Array[Double]) extends DVector with Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDVector2 =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

object MyDVector {
  def unapply(dv: MyDVector): Option[Array[Double]] = Some(dv.data)
}

object MyDVector2 {
  def unapply(dv: MyDVector2): Option[Array[Double]] = Some(dv.data)
}

@BeanInfo
private[sql] case class MyPoint(
    @BeanProperty label: Double,
    @BeanProperty features: DVector)

private[sql] class TestUserClass {
}

private[sql] class NonUserDefinedType {
}

private[sql] class MyDVectorUDT extends UserDefinedType[DVector] {

  override def sqlType: DataType = {
    // type: 0 = MyDVector, 1 = MyDVector2
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = false)))
  }

  override def serialize(features: DVector): InternalRow = {
    val row = new GenericMutableRow(2)
    features match {
      case MyDVector(data) =>
        row.setByte(0, 0)
        row.update(1, new GenericArrayData(data.map(_.asInstanceOf[Any])))
      case MyDVector2(data) =>
        row.setByte(0, 1)
        row.update(1, new GenericArrayData(data.map(_.asInstanceOf[Any])))
    }
    row
  }

  override def deserialize(datum: Any): DVector = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 2, "MyDVectorUDT.deserialize given row with length " +
          s"${row.numFields} but requires length == 2")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val values = row.getArray(1).toDoubleArray()
            new MyDVector(values)
          case 1 =>
            val values = row.getArray(1).toDoubleArray()
            new MyDVector2(values)
        }
    }
  }

  override def userClass: Class[DVector] = classOf[DVector]

  private[spark] override def asNullable: MyDVectorUDT = this

  override def equals(other: Any): Boolean = other match {
    case _: MyDVectorUDT => true
    case _ => false
  }
}

class UDTRegistrationSuite extends QueryTest with SharedSQLContext with ParquetTest {
  import testImplicits._

  UDTRegistration.register(classOf[DVector], classOf[MyDVectorUDT])
  UDTRegistration.register(classOf[MyDVector], classOf[MyDVectorUDT])
  UDTRegistration.register(classOf[MyDVector2], classOf[MyDVectorUDT])

  private lazy val pointsRDD = Seq(
    MyPoint(1.0, new MyDVector(Array(0.1, 1.0))),
    MyPoint(0.0, new MyDVector(Array(0.2, 2.0)))).toDF()

  private lazy val pointsRDD2 = Seq(
    MyPoint(1.0, new MyDVector2(Array(0.1, 1.0))),
    MyPoint(0.0, new MyDVector2(Array(0.2, 2.0)))).toDF()

  test("register non-UserDefinedType") {
    intercept[SparkException] {
      UDTRegistration.register(classOf[TestUserClass], classOf[NonUserDefinedType])
    }
    UDTRegistration.register(classOf[TestUserClass], "org.apache.spark.sql.NonUserDefinedType")
    intercept[SparkException] {
      UDTRegistration.getUDTFor(classOf[TestUserClass])
    }
  }

  test("register user type: MyDVector for MyPoint") {
    val labels: RDD[Double] = pointsRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDVector] =
      pointsRDD.select('features).rdd.map { case Row(v: MyDVector) => v }
    val featuresArrays: Array[MyDVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDVector(Array(0.2, 2.0))))
  }

  test("user type registered with UDTRegistration: MyDVector2 for MyPoint") {
    val labels: RDD[Double] = pointsRDD2.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDVector2] =
      pointsRDD2.select('features).rdd.map { case Row(v: MyDVector2) => v }
    val featuresArrays: Array[MyDVector2] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDVector2(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDVector2(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    sqlContext.udf.register("testType", (d: MyDVector) => d.isInstanceOf[MyDVector])
    pointsRDD.registerTempTable("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }

  test("UDTs and UDFs through UDTRegistration") {
    sqlContext.udf.register("testType", (d: MyDVector2) => d.isInstanceOf[MyDVector2])
    pointsRDD2.registerTempTable("points2")
    checkAnswer(
      sql("SELECT testType(features) from points2"),
      Seq(Row(true), Row(true)))
  }

  testStandardAndLegacyModes("UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDVector(Array(0.1, 1.0))),
          Row(0.0, new MyDVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("UDTs with Parquet through UDTRegistration") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD2.write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDVector2(Array(0.1, 1.0))),
          Row(0.0, new MyDVector2(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.repartition(1).write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDVector(Array(0.1, 1.0))),
          Row(0.0, new MyDVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet through UDTRegistration") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD2.repartition(1).write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDVector2(Array(0.1, 1.0))),
          Row(0.0, new MyDVector2(Array(0.2, 2.0)))))
    }
  }

  // Tests to make sure that all operators correctly convert types on the way out.
  test("Local UDTs") {
    val df = Seq((1, new MyDVector(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[MyDVector](1)
    df.take(1)(0).getAs[MyDVector](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDVector](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDVector](0)
  }

  test("Local UDTs through UDTRegistration") {
    val df = Seq((1, new MyDVector2(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[MyDVector2](1)
    df.take(1)(0).getAs[MyDVector2](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDVector2](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDVector2](0)
  }

  test("UDTs with JSON") {
    val data = Seq(
      "{\"id\":1,\"vec\":{\"type\":0, \"values\":[1.1,2.2,3.3,4.4]}}",
      "{\"id\":2,\"vec\":{\"type\":0, \"values\":[2.25,4.5,8.75]}}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new MyDVectorUDT, false)
    ))

    val stringRDD = sparkContext.parallelize(data)
    val jsonRDD = sqlContext.read.schema(schema).json(stringRDD)
    checkAnswer(
      jsonRDD,
      Row(1, new MyDVector(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new MyDVector(Array(2.25, 4.5, 8.75))) ::
        Nil
    )
  }

  test("UDTs with JSON through UDTRegistration") {
    val data = Seq(
      "{\"id\":1,\"vec\":{\"type\":1, \"values\":[1.1,2.2,3.3,4.4]}}",
      "{\"id\":2,\"vec\":{\"type\":1, \"values\":[2.25,4.5,8.75]}}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new MyDVectorUDT, false)
    ))

    val stringRDD = sparkContext.parallelize(data)
    val jsonRDD = sqlContext.read.schema(schema).json(stringRDD)
    checkAnswer(
      jsonRDD,
      Row(1, new MyDVector2(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new MyDVector2(Array(2.25, 4.5, 8.75))) ::
        Nil
    )
  }

  test("Catalyst type converter null handling for UDTs") {
    val udt = new MyDVectorUDT()
    val toScalaConverter = CatalystTypeConverters.createToScalaConverter(udt)
    assert(toScalaConverter(null) === null)

    val toCatalystConverter = CatalystTypeConverters.createToCatalystConverter(udt)
    assert(toCatalystConverter(null) === null)

  }
}
