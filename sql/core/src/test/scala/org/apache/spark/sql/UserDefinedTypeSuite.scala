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

import java.time.Year
import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Cast, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

private[sql] case class MyLabeledPoint(label: Double, features: TestUDT.MyDenseVector) {
  def getLabel: Double = label
  def getFeatures: TestUDT.MyDenseVector = features
}

private[sql] case class FooWithDate(year: Year, s: String, i: Int)

private[sql] class YearUDT extends UserDefinedType[Year] {
  override def sqlType: DataType = IntegerType

  override def serialize(obj: Year): Int = {
    obj.getValue
  }

  def deserialize(datum: Any): Year = datum match {
    case value: Int => Year.of(value)
  }

  override def userClass: Class[Year] = classOf[Year]

  private[spark] override def asNullable: YearUDT = this
}

class UserDefinedTypeSuite extends QueryTest with SharedSparkSession with ParquetTest
    with ExpressionEvalHelper {
  import testImplicits._

  private lazy val pointsRDD = Seq(
    MyLabeledPoint(1.0, new TestUDT.MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new TestUDT.MyDenseVector(Array(0.2, 2.0)))).toDF()

  private lazy val pointsRDD2 = Seq(
    MyLabeledPoint(1.0, new TestUDT.MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new TestUDT.MyDenseVector(Array(0.3, 3.0)))).toDF()


  test("SPARK-32090: equal") {
    val udt1 = new ExampleBaseTypeUDT
    val udt2 = new ExampleSubTypeUDT
    val udt3 = new ExampleSubTypeUDT
    assert(udt1 !== udt2)
    assert(udt2 !== udt1)
    assert(udt2 === udt3)
    assert(udt3 === udt2)
  }

  test("SPARK-32090: acceptsType") {
    val udt1 = new ExampleBaseTypeUDT
    val udt2 = new ExampleSubTypeUDT
    assert(udt1.acceptsType(udt2))
    assert(!udt2.acceptsType(udt1))
  }

  test("register user type: MyDenseVector for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD.select($"label").rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[TestUDT.MyDenseVector] =
      pointsRDD.select($"features").rdd.map { case Row(v: TestUDT.MyDenseVector) => v }
    val featuresArrays: Array[TestUDT.MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new TestUDT.MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new TestUDT.MyDenseVector(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    withTempView("points") {
      spark.udf.register("testType",
        (d: TestUDT.MyDenseVector) => d.isInstanceOf[TestUDT.MyDenseVector])
      pointsRDD.createOrReplaceTempView("points")
      checkAnswer(
        sql("SELECT testType(features) from points"),
        Seq(Row(true), Row(true)))
    }
  }

  testStandardAndLegacyModes("UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.write.parquet(path)
      checkAnswer(
        spark.read.parquet(path),
        Seq(
          Row(1.0, new TestUDT.MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new TestUDT.MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.repartition(1).write.parquet(path)
      checkAnswer(
        spark.read.parquet(path),
        Seq(
          Row(1.0, new TestUDT.MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new TestUDT.MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  // Tests to make sure that all operators correctly convert types on the way out.
  test("Local UDTs") {
    val vec = new TestUDT.MyDenseVector(Array(0.1, 1.0))
    val df = Seq((1, vec)).toDF("int", "vec")
    assert(vec === df.collect()(0).getAs[TestUDT.MyDenseVector](1))
    assert(vec === df.take(1)(0).getAs[TestUDT.MyDenseVector](1))
    checkAnswer(df.limit(1).groupBy($"int").agg(first($"vec")), Row(1, vec))
    checkAnswer(df.orderBy($"int").limit(1).groupBy($"int")
      .agg(first($"vec")), Row(1, vec))
  }

  test("UDTs with JSON") {
    val data = Seq(
      "{\"id\":1,\"vec\":[1.1,2.2,3.3,4.4]}",
      "{\"id\":2,\"vec\":[2.25,4.5,8.75]}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new TestUDT.MyDenseVectorUDT, false)
    ))

    val jsonRDD = spark.read.schema(schema).json(data.toDS())
    checkAnswer(
      jsonRDD,
      Row(1, new TestUDT.MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new TestUDT.MyDenseVector(Array(2.25, 4.5, 8.75))) ::
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
      StructField("vec", new TestUDT.MyDenseVectorUDT, false)
    ))

    val jsonDataset = spark.read.schema(schema).json(data.toDS())
      .as[(Int, TestUDT.MyDenseVector)]
    checkDataset(
      jsonDataset,
      (1, new TestUDT.MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))),
      (2, new TestUDT.MyDenseVector(Array(2.25, 4.5, 8.75)))
    )
  }

  test("SPARK-10472 UserDefinedType.typeName") {
    assert(IntegerType.typeName === "integer")
    assert(new TestUDT.MyDenseVectorUDT().typeName === "mydensevector")
  }

  test("Catalyst type converter null handling for UDTs") {
    val udt = new TestUDT.MyDenseVectorUDT()
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
    checkAnswer(sql("SELECT doOtherUDF(doUDF(41))"), Row(41) :: Nil)

    // this one passes only with the fix SPARK-19311:
    // return type of doSubUDF is a subtype of the parameter type of doOtherUDF
    checkAnswer(sql("SELECT doOtherUDF(doSubTypeUDF(42))"), Row(42) :: Nil)
  }

  test("except on UDT") {
    checkAnswer(
      pointsRDD.except(pointsRDD2),
      Seq(Row(0.0, new TestUDT.MyDenseVector(Array(0.2, 2.0)))))
  }

  test("SPARK-23054 Cast UserDefinedType to string") {
    val udt = new TestUDT.MyDenseVectorUDT()
    val vector = new TestUDT.MyDenseVector(Array(1.0, 3.0, 5.0, 7.0, 9.0))
    val data = udt.serialize(vector)
    val ret = Cast(Literal(data, udt), StringType, None)
    checkEvaluation(ret, "(1.0, 3.0, 5.0, 7.0, 9.0)")
  }

  test("SPARK-28497 Can't up cast UserDefinedType to string") {
    val udt = new TestUDT.MyDenseVectorUDT()
    assert(!Cast.canUpCast(udt, StringType))
  }

  test("typeof user defined type") {
    val schema = new StructType().add("a", new TestUDT.MyDenseVectorUDT())
    val data = Arrays.asList(
      RowFactory.create(new TestUDT.MyDenseVector(Array(1.0, 3.0, 5.0, 7.0, 9.0))))
    checkAnswer(spark.createDataFrame(data, schema).selectExpr("typeof(a)"),
      Seq(Row("array<double>")))
  }

  test("SPARK-30993: UserDefinedType matched to fixed length SQL type shouldn't be corrupted") {
    def concatFoo(a: FooWithDate, b: FooWithDate): FooWithDate = {
      FooWithDate(b.year, a.s + b.s, a.i)
    }

    UDTRegistration.register(classOf[Year].getName, classOf[YearUDT].getName)

    val year = Year.now()
    val inputDS = List(FooWithDate(year, "Foo", 1), FooWithDate(year, "Foo", 3),
      FooWithDate(year, "Foo", 3)).toDS()
    val agg = inputDS.groupByKey(x => x.i).mapGroups((_, iter) => iter.reduce(concatFoo))
    val result = agg.collect()

    assert(result.toSet === Set(FooWithDate(year, "FooFoo", 3), FooWithDate(year, "Foo", 1)))
  }
}
