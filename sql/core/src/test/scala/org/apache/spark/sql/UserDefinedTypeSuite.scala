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
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

private[sql] trait DenseVector extends Serializable

@SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
private[sql] class MyDenseVector(val data: Array[Double]) extends DenseVector with Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

private[sql] class MyDenseVector2(val data: Array[Double]) extends DenseVector with Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector2 =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

object MyDenseVector {
  def unapply(dv: MyDenseVector): Option[Array[Double]] = Some(dv.data)
}

object MyDenseVector2 {
  def unapply(dv: MyDenseVector2): Option[Array[Double]] = Some(dv.data)
}

@BeanInfo
private[sql] case class MyLabeledPoint(
    @BeanProperty label: Double,
    @BeanProperty features: DenseVector)

private[sql] class MyDenseVectorUDT extends UserDefinedType[DenseVector] {

  override def sqlType: DataType = {
    // type: 0 = MyDenseVector, 1 = MyDenseVector2
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = false)))
  }

  override def serialize(features: DenseVector): InternalRow = {
    val row = new GenericMutableRow(2)
    features match {
      case MyDenseVector(data) =>
        row.setByte(0, 0)
        row.update(1, new GenericArrayData(data.map(_.asInstanceOf[Any])))
      case MyDenseVector2(data) =>
        row.setByte(0, 1)
        row.update(1, new GenericArrayData(data.map(_.asInstanceOf[Any])))
    }
    row
  }

  override def deserialize(datum: Any): DenseVector = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 2, "MyDenseVectorUDT.deserialize given row with length " +
          s"${row.numFields} but requires length == 2")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val values = row.getArray(1).toDoubleArray()
            new MyDenseVector(values)
          case 1 =>
            val values = row.getArray(1).toDoubleArray()
            new MyDenseVector2(values)
        }
    }
  }

  override def userClass: Class[DenseVector] = classOf[DenseVector]

  private[spark] override def asNullable: MyDenseVectorUDT = this

  override def equals(other: Any): Boolean = other match {
    case _: MyDenseVectorUDT => true
    case _ => false
  }
}

class UserDefinedTypeSuite extends QueryTest with SharedSQLContext with ParquetTest {
  import testImplicits._

  UDTRegistration.register(classOf[DenseVector], classOf[MyDenseVectorUDT])
  UDTRegistration.register(classOf[MyDenseVector2], classOf[MyDenseVectorUDT])

  private lazy val pointsRDD = Seq(
    MyLabeledPoint(1.0, new MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new MyDenseVector(Array(0.2, 2.0)))).toDF()

  private lazy val pointsRDD2 = Seq(
    MyLabeledPoint(1.0, new MyDenseVector2(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new MyDenseVector2(Array(0.2, 2.0)))).toDF()

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

  test("user type registered with UDTRegistration: MyDenseVector2 for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD2.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDenseVector2] =
      pointsRDD2.select('features).rdd.map { case Row(v: MyDenseVector2) => v }
    val featuresArrays: Array[MyDenseVector2] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector2(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector2(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    sqlContext.udf.register("testType", (d: MyDenseVector) => d.isInstanceOf[MyDenseVector])
    pointsRDD.registerTempTable("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }

  test("UDTs and UDFs through UDTRegistration") {
    sqlContext.udf.register("testType", (d: MyDenseVector2) => d.isInstanceOf[MyDenseVector2])
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
          Row(1.0, new MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("UDTs with Parquet through UDTRegistration") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD2.write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDenseVector2(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector2(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.repartition(1).write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet through UDTRegistration") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD2.repartition(1).write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDenseVector2(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector2(Array(0.2, 2.0)))))
    }
  }

  // Tests to make sure that all operators correctly convert types on the way out.
  test("Local UDTs") {
    val df = Seq((1, new MyDenseVector(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[MyDenseVector](1)
    df.take(1)(0).getAs[MyDenseVector](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector](0)
  }

  test("Local UDTs through UDTRegistration") {
    val df = Seq((1, new MyDenseVector2(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[MyDenseVector2](1)
    df.take(1)(0).getAs[MyDenseVector2](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector2](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector2](0)
  }

  test("UDTs with JSON") {
    val data = Seq(
      "{\"id\":1,\"vec\":{\"type\":0, \"values\":[1.1,2.2,3.3,4.4]}}",
      "{\"id\":2,\"vec\":{\"type\":0, \"values\":[2.25,4.5,8.75]}}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new MyDenseVectorUDT, false)
    ))

    val stringRDD = sparkContext.parallelize(data)
    val jsonRDD = sqlContext.read.schema(schema).json(stringRDD)
    checkAnswer(
      jsonRDD,
      Row(1, new MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new MyDenseVector(Array(2.25, 4.5, 8.75))) ::
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
      StructField("vec", new MyDenseVectorUDT, false)
    ))

    val stringRDD = sparkContext.parallelize(data)
    val jsonRDD = sqlContext.read.schema(schema).json(stringRDD)
    checkAnswer(
      jsonRDD,
      Row(1, new MyDenseVector2(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new MyDenseVector2(Array(2.25, 4.5, 8.75))) ::
        Nil
    )
  }

  test("SPARK-10472 UserDefinedType.typeName") {
    assert(IntegerType.typeName === "integer")
    assert(new MyDenseVectorUDT().typeName === "mydensevector")
  }

  test("Catalyst type converter null handling for UDTs") {
    val udt = new MyDenseVectorUDT()
    val toScalaConverter = CatalystTypeConverters.createToScalaConverter(udt)
    assert(toScalaConverter(null) === null)

    val toCatalystConverter = CatalystTypeConverters.createToCatalystConverter(udt)
    assert(toCatalystConverter(null) === null)

  }
}
