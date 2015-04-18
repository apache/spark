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

package org.apache.spark.sql.execution

import java.sql.{Timestamp, Date}

import org.scalatest.{FunSuite, BeforeAndAfterAll}

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.ShuffleDependency
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.{MyDenseVectorUDT, QueryTest}

class SparkSqlSerializer2DataTypeSuite extends FunSuite {
  // Make sure that we will not use serializer2 for unsupported data types.
  def checkSupported(dataType: DataType, isSupported: Boolean): Unit = {
    val testName =
      s"${if (dataType == null) null else dataType.toString} is " +
        s"${if (isSupported) "supported" else "unsupported"}"

    test(testName) {
      assert(SparkSqlSerializer2.support(Array(dataType)) === isSupported)
    }
  }

  checkSupported(null, isSupported = true)
  checkSupported(NullType, isSupported = true)
  checkSupported(BooleanType, isSupported = true)
  checkSupported(ByteType, isSupported = true)
  checkSupported(ShortType, isSupported = true)
  checkSupported(IntegerType, isSupported = true)
  checkSupported(LongType, isSupported = true)
  checkSupported(FloatType, isSupported = true)
  checkSupported(DoubleType, isSupported = true)
  checkSupported(DateType, isSupported = true)
  checkSupported(TimestampType, isSupported = true)
  checkSupported(StringType, isSupported = true)
  checkSupported(BinaryType, isSupported = true)
  checkSupported(DecimalType(10, 5), isSupported = true)
  checkSupported(DecimalType.Unlimited, isSupported = true)

  // For now, ArrayType, MapType, and StructType are not supported.
  checkSupported(ArrayType(DoubleType, true), isSupported = false)
  checkSupported(ArrayType(StringType, false), isSupported = false)
  checkSupported(MapType(IntegerType, StringType, true), isSupported = false)
  checkSupported(MapType(IntegerType, ArrayType(DoubleType), false), isSupported = false)
  checkSupported(StructType(StructField("a", IntegerType, true) :: Nil), isSupported = false)
  // UDTs are not supported right now.
  checkSupported(new MyDenseVectorUDT, isSupported = false)
}

abstract class SparkSqlSerializer2Suite extends QueryTest with BeforeAndAfterAll {
  var allColumns: String = _
  val serializerClass: Class[Serializer] =
    classOf[SparkSqlSerializer2].asInstanceOf[Class[Serializer]]
  var numShufflePartitions: Int = _
  var useSerializer2: Boolean = _

  override def beforeAll(): Unit = {
    numShufflePartitions = conf.numShufflePartitions
    useSerializer2 = conf.useSqlSerializer2

    sql("set spark.sql.useSerializer2=true")

    val supportedTypes =
      Seq(StringType, BinaryType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType.Unlimited, DecimalType(6, 5),
        DateType, TimestampType)

    val fields = supportedTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    allColumns = fields.map(_.name).mkString(",")
    val schema = StructType(fields)

    // Create a RDD with all data types supported by SparkSqlSerializer2.
    val rdd =
      sparkContext.parallelize((1 to 1000), 10).map { i =>
        Row(
          s"str${i}: test serializer2.",
          s"binary${i}: test serializer2.".getBytes("UTF-8"),
          null,
          i % 2 == 0,
          i.toByte,
          i.toShort,
          i,
          Long.MaxValue - i.toLong,
          (i + 0.25).toFloat,
          (i + 0.75),
          BigDecimal(Long.MaxValue.toString + ".12345"),
          new java.math.BigDecimal(s"${i % 9 + 1}" + ".23456"),
          new Date(i),
          new Timestamp(i))
      }

    createDataFrame(rdd, schema).registerTempTable("shuffle")

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    dropTempTable("shuffle")
    sql(s"set spark.sql.shuffle.partitions=$numShufflePartitions")
    sql(s"set spark.sql.useSerializer2=$useSerializer2")
    super.afterAll()
  }

  def checkSerializer[T <: Serializer](
      executedPlan: SparkPlan,
      expectedSerializerClass: Class[T]): Unit = {
    executedPlan.foreach {
      case exchange: Exchange =>
        val shuffledRDD = exchange.execute().firstParent.asInstanceOf[ShuffledRDD[_, _, _]]
        val dependency = shuffledRDD.getDependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
        val serializerNotSetMessage =
          s"Expected $expectedSerializerClass as the serializer of Exchange. " +
          s"However, the serializer was not set."
        val serializer = dependency.serializer.getOrElse(fail(serializerNotSetMessage))
        assert(serializer.getClass === expectedSerializerClass)
      case _ => // Ignore other nodes.
    }
  }

  test("key schema and value schema are not nulls") {
    val df = sql(s"SELECT DISTINCT ${allColumns} FROM shuffle")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    checkAnswer(
      df,
      table("shuffle").collect())
  }

  test("value schema is null") {
    val df = sql(s"SELECT col0 FROM shuffle ORDER BY col0")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    assert(
      df.map(r => r.getString(0)).collect().toSeq ===
      table("shuffle").select("col0").map(r => r.getString(0)).collect().sorted.toSeq)
  }

  test("no map output field") {
    val df = sql(s"SELECT 1 + 1 FROM shuffle")
    checkSerializer(df.queryExecution.executedPlan, classOf[SparkSqlSerializer])
  }
}

/** Tests SparkSqlSerializer2 with sort based shuffle without sort merge. */
class SparkSqlSerializer2SortShuffleSuite extends SparkSqlSerializer2Suite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Sort merge will not be triggered.
    sql("set spark.sql.shuffle.partitions = 200")
  }

  test("key schema is null") {
    val aggregations = allColumns.split(",").map(c => s"COUNT($c)").mkString(",")
    val df = sql(s"SELECT $aggregations FROM shuffle")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    checkAnswer(
      df,
      Row(1000, 1000, 0, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000))
  }
}

/** For now, we will use SparkSqlSerializer for sort based shuffle with sort merge. */
class SparkSqlSerializer2SortMergeShuffleSuite extends SparkSqlSerializer2Suite {

  // We are expecting SparkSqlSerializer.
  override val serializerClass: Class[Serializer] =
    classOf[SparkSqlSerializer].asInstanceOf[Class[Serializer]]

  override def beforeAll(): Unit = {
    super.beforeAll()
    // To trigger the sort merge.
    sql("set spark.sql.shuffle.partitions = 201")
  }
}
