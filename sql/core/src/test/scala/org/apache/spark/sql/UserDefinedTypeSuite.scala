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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

@BeanInfo
private[sql] case class MyLabeledPoint(
  @BeanProperty label: Double,
  @BeanProperty features: UDT.MyDenseVector)

// Wrapped in an object to check Scala compatibility. See SPARK-13929
object UDT {

  @SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
  private[sql] class MyDenseVector(val data: Array[Double]) extends Serializable {
    override def hashCode(): Int = java.util.Arrays.hashCode(data)

    override def equals(other: Any): Boolean = other match {
      case v: MyDenseVector => java.util.Arrays.equals(this.data, v.data)
      case _ => false
    }
  }

  private[sql] class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

    override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

    override def serialize(features: MyDenseVector): ArrayData = {
      new GenericArrayData(features.data.map(_.asInstanceOf[Any]))
    }

    override def deserialize(datum: Any): MyDenseVector = {
      datum match {
        case data: ArrayData =>
          new MyDenseVector(data.toDoubleArray())
      }
    }

    override def userClass: Class[MyDenseVector] = classOf[MyDenseVector]

    private[spark] override def asNullable: MyDenseVectorUDT = this

    override def hashCode(): Int = getClass.hashCode()

    override def equals(other: Any): Boolean = other.isInstanceOf[MyDenseVectorUDT]
  }

}

// object and classes to test SPARK-19311

// Trait/Interface for base type
sealed trait IExampleBaseType extends Serializable {
  def field: Int
}

// Trait/Interface for derived type
sealed trait IExampleSubType extends IExampleBaseType

// a base class
class ExampleBaseClass(override val field: Int) extends IExampleBaseType

// a derived class
class ExampleSubClass(override val field: Int)
  extends ExampleBaseClass(field) with IExampleSubType

// UDT for base class
class ExampleBaseTypeUDT extends UserDefinedType[IExampleBaseType] {

  override def sqlType: StructType = {
    StructType(Seq(
      StructField("intfield", IntegerType, nullable = false)))
  }

  override def serialize(obj: IExampleBaseType): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setInt(0, obj.field)
    row
  }

  override def deserialize(datum: Any): IExampleBaseType = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          "ExampleBaseTypeUDT requires row with length == 1")
        val field = row.getInt(0)
        new ExampleBaseClass(field)
    }
  }

  override def userClass: Class[IExampleBaseType] = classOf[IExampleBaseType]
}

// UDT for derived class
private[spark] class ExampleSubTypeUDT extends UserDefinedType[IExampleSubType] {

  override def sqlType: StructType = {
    StructType(Seq(
      StructField("intfield", IntegerType, nullable = false)))
  }

  override def serialize(obj: IExampleSubType): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setInt(0, obj.field)
    row
  }

  override def deserialize(datum: Any): IExampleSubType = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          "ExampleSubTypeUDT requires row with length == 1")
        val field = row.getInt(0)
        new ExampleSubClass(field)
    }
  }

  override def userClass: Class[IExampleSubType] = classOf[IExampleSubType]
}

class UserDefinedTypeSuite extends QueryTest with SharedSQLContext with ParquetTest {
  import testImplicits._

  private lazy val pointsRDD = Seq(
    MyLabeledPoint(1.0, new UDT.MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new UDT.MyDenseVector(Array(0.2, 2.0)))).toDF()

  private lazy val pointsRDD2 = Seq(
    MyLabeledPoint(1.0, new UDT.MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new UDT.MyDenseVector(Array(0.3, 3.0)))).toDF()

  test("register user type: MyDenseVector for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[UDT.MyDenseVector] =
      pointsRDD.select('features).rdd.map { case Row(v: UDT.MyDenseVector) => v }
    val featuresArrays: Array[UDT.MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new UDT.MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new UDT.MyDenseVector(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    spark.udf.register("testType", (d: UDT.MyDenseVector) => d.isInstanceOf[UDT.MyDenseVector])
    pointsRDD.createOrReplaceTempView("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }

  testStandardAndLegacyModes("UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.write.parquet(path)
      checkAnswer(
        spark.read.parquet(path),
        Seq(
          Row(1.0, new UDT.MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new UDT.MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.repartition(1).write.parquet(path)
      checkAnswer(
        spark.read.parquet(path),
        Seq(
          Row(1.0, new UDT.MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new UDT.MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  // Tests to make sure that all operators correctly convert types on the way out.
  test("Local UDTs") {
    val df = Seq((1, new UDT.MyDenseVector(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[UDT.MyDenseVector](1)
    df.take(1)(0).getAs[UDT.MyDenseVector](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[UDT.MyDenseVector](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0)
      .getAs[UDT.MyDenseVector](0)
  }

  test("UDTs with JSON") {
    val data = Seq(
      "{\"id\":1,\"vec\":[1.1,2.2,3.3,4.4]}",
      "{\"id\":2,\"vec\":[2.25,4.5,8.75]}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new UDT.MyDenseVectorUDT, false)
    ))

    val jsonRDD = spark.read.schema(schema).json(data.toDS())
    checkAnswer(
      jsonRDD,
      Row(1, new UDT.MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new UDT.MyDenseVector(Array(2.25, 4.5, 8.75))) ::
        Nil
    )
  }

  test("UDTs with JSON and Dataset") {
    val data = Seq(
      "{\"id\":1,\"vec\":[1.1,2.2,3.3,4.4]}",
      "{\"id\":2,\"vec\":[2.25,4.5,8.75]}"
    )

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new UDT.MyDenseVectorUDT, false)
    ))

    val jsonDataset = spark.read.schema(schema).json(data.toDS())
      .as[(Int, UDT.MyDenseVector)]
    checkDataset(
      jsonDataset,
      (1, new UDT.MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))),
      (2, new UDT.MyDenseVector(Array(2.25, 4.5, 8.75)))
    )
  }

  test("SPARK-10472 UserDefinedType.typeName") {
    assert(IntegerType.typeName === "integer")
    assert(new UDT.MyDenseVectorUDT().typeName === "mydensevector")
  }

  test("Catalyst type converter null handling for UDTs") {
    val udt = new UDT.MyDenseVectorUDT()
    val toScalaConverter = CatalystTypeConverters.createToScalaConverter(udt)
    assert(toScalaConverter(null) === null)

    val toCatalystConverter = CatalystTypeConverters.createToCatalystConverter(udt)
    assert(toCatalystConverter(null) === null)
  }

  test("SPARK-15658: Analysis exception if Dataset.map returns UDT object") {
    // call `collect` to make sure this query can pass analysis.
    pointsRDD.as[MyLabeledPoint].map(_.copy(label = 2.0)).collect()
  }

  test("SPARK-19311: UDFs disregard UDT type hierarchy") {
    UDTRegistration.register(classOf[IExampleBaseType].getName,
      classOf[ExampleBaseTypeUDT].getName)
    UDTRegistration.register(classOf[IExampleSubType].getName,
      classOf[ExampleSubTypeUDT].getName)

    // UDF that returns a base class object
    sqlContext.udf.register("doUDF", (param: Int) => {
      new ExampleBaseClass(param)
    }: IExampleBaseType)

    // UDF that returns a derived class object
    sqlContext.udf.register("doSubTypeUDF", (param: Int) => {
      new ExampleSubClass(param)
    }: IExampleSubType)

    // UDF that takes a base class object as parameter
    sqlContext.udf.register("doOtherUDF", (obj: IExampleBaseType) => {
      obj.field
    }: Int)

    // this worked already before the fix SPARK-19311:
    // return type of doUDF equals parameter type of doOtherUDF
    sql("SELECT doOtherUDF(doUDF(41))")

    // this one passes only with the fix SPARK-19311:
    // return type of doSubUDF is a subtype of the parameter type of doOtherUDF
    sql("SELECT doOtherUDF(doSubTypeUDF(42))")
  }

  test("except on UDT") {
    checkAnswer(
      pointsRDD.except(pointsRDD2),
      Seq(Row(0.0, new UDT.MyDenseVector(Array(0.2, 2.0)))))
  }
}
