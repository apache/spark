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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DeprecatedAPISuite extends QueryTest with SharedSparkSession {
  import MathFunctionsTestData.DoubleData
  import testImplicits._

  private lazy val doubleData = (1 to 10).map(i => DoubleData(i * 0.2 - 1, i * -0.2 + 1)).toDF()

  private def testOneToOneMathFunction[
    @specialized(Int, Long, Float, Double) T,
    @specialized(Int, Long, Float, Double) U](
        c: Column => Column,
        f: T => U): Unit = {
    checkAnswer(
      doubleData.select(c('a)),
      (1 to 10).map(n => Row(f((n * 0.2 - 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c('b)),
      (1 to 10).map(n => Row(f((-n * 0.2 + 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c(lit(null))),
      (1 to 10).map(_ => Row(null))
    )
  }

  test("functions.toDegrees") {
    testOneToOneMathFunction(toDegrees, math.toDegrees)
    withView("t") {
      val df = Seq(0, 1, 1.5).toDF("a")
      df.createOrReplaceTempView("t")

      checkAnswer(
        sql("SELECT degrees(0), degrees(1), degrees(1.5)"),
        Seq(0).toDF().select(toDegrees(lit(0)), toDegrees(lit(1)), toDegrees(lit(1.5)))
      )
      checkAnswer(
        sql("SELECT degrees(a) FROM t"),
        df.select(toDegrees("a"))
      )
    }
  }

  test("functions.toRadians") {
    testOneToOneMathFunction(toRadians, math.toRadians)
    withView("t") {
      val df = Seq(0, 1, 1.5).toDF("a")
      df.createOrReplaceTempView("t")

      checkAnswer(
        sql("SELECT radians(0), radians(1), radians(1.5)"),
        Seq(0).toDF().select(toRadians(lit(0)), toRadians(lit(1)), toRadians(lit(1.5)))
      )
      checkAnswer(
        sql("SELECT radians(a) FROM t"),
        df.select(toRadians("a"))
      )
    }
  }

  test("functions.approxCountDistinct") {
    withView("t") {
      val df = Seq(0, 1, 2).toDF("a")
      df.createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT approx_count_distinct(a) FROM t"),
        df.select(approxCountDistinct("a")))
    }
  }

  test("functions.monotonicallyIncreasingId") {
    // Make sure we have 2 partitions, each with 2 records.
    val df = sparkContext.parallelize(Seq[Int](), 2).mapPartitions { _ =>
      Iterator(Tuple1(1), Tuple1(2))
    }.toDF("a")
    checkAnswer(
      df.select(monotonicallyIncreasingId(), expr("monotonically_increasing_id()")),
      Row(0L, 0L) ::
        Row(1L, 1L) ::
        Row((1L << 33) + 0L, (1L << 33) + 0L) ::
        Row((1L << 33) + 1L, (1L << 33) + 1L) :: Nil
    )
  }

  test("Column.!==") {
    val nullData = Seq(
      (Some(1), Some(1)), (Some(1), Some(2)), (Some(1), None), (None, None)).toDF("a", "b")
    checkAnswer(
      nullData.filter($"b" !== 1),
      Row(1, 2) :: Nil)

    checkAnswer(nullData.filter($"b" !== null), Nil)

    checkAnswer(
      nullData.filter($"a" !== $"b"),
      Row(1, 2) :: Nil)
  }

  test("Dataset.registerTempTable") {
    withTempView("t") {
      Seq(1).toDF().registerTempTable("t")
      assert(spark.catalog.tableExists("t"))
    }
  }

  test("SQLContext.setActive/clearActive") {
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    SQLContext.setActive(sqlContext)
    assert(SparkSession.getActiveSession === Some(spark))
    SQLContext.clearActive()
    assert(SparkSession.getActiveSession === None)
  }

  test("SQLContext.applySchema") {
    val rowRdd = sparkContext.parallelize(Seq(Row("Jack", 20), Row("Marry", 18)))
    val schema = StructType(StructField("name", StringType, false) ::
      StructField("age", IntegerType, true) :: Nil)
    val sqlContext = spark.sqlContext
    checkAnswer(sqlContext.applySchema(rowRdd, schema), Row("Jack", 20) :: Row("Marry", 18) :: Nil)
    checkAnswer(sqlContext.applySchema(rowRdd.toJavaRDD(), schema),
      Row("Jack", 20) :: Row("Marry", 18) :: Nil)
  }

  test("SQLContext.parquetFile") {
    val sqlContext = spark.sqlContext
    withTempDir { dir =>
      val parquetFile = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.parquet(parquetFile)
      val parquetDF = sqlContext.parquetFile(parquetFile)
      checkAnswer(parquetDF, expectDF)
    }
  }

  test("SQLContext.jsonFile") {
    val sqlContext = spark.sqlContext
    withTempDir { dir =>
      val jsonFile = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.json(jsonFile)
      var jsonDF = sqlContext.jsonFile(jsonFile)
      checkAnswer(jsonDF, expectDF)
      assert(jsonDF.schema === expectDF.schema.asNullable)

      var schema = expectDF.schema
      jsonDF = sqlContext.jsonFile(jsonFile, schema)
      checkAnswer(jsonDF, expectDF)
      assert(jsonDF.schema === schema.asNullable)

      jsonDF = sqlContext.jsonFile(jsonFile, 0.9)
      checkAnswer(jsonDF, expectDF)

      val jsonRDD = sparkContext.parallelize(Seq("{\"name\":\"Jack\",\"age\":20}",
        "{\"name\":\"Marry\",\"age\":18}"))
      jsonDF = sqlContext.jsonRDD(jsonRDD)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD())
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)

      schema = StructType(StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD, schema)
      checkAnswer(jsonDF, Row("Jack", 20) :: Row("Marry", 18) :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD(), schema)
      checkAnswer(jsonDF, Row("Jack", 20) :: Row("Marry", 18) :: Nil)


      jsonDF = sqlContext.jsonRDD(jsonRDD, 0.9)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD(), 0.9)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
    }
  }

  test("SQLContext.load") {
    withTempDir { dir =>
      val path = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.parquet(path)
      val sqlContext = spark.sqlContext

      var loadDF = sqlContext.load(path)
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load(path, "parquet")
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load("parquet", Map("path" -> path))
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load("parquet", expectDF.schema, Map("path" -> path))
      checkAnswer(loadDF, expectDF)
    }
  }
}
