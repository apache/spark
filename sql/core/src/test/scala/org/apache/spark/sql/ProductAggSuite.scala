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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, product}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, ShortType}


class ProductAggSuite extends QueryTest
  with SharedSparkSession {

  // Sequence of integers small enough that factorial is representable exactly as DoubleType:
  private lazy val data16 = spark.range(1, 17).toDF("x")

  private lazy val factorials = (1 to 16).scanLeft(1L) { case (f, x) => f * x }

  test("bare factorial") {
    checkAnswer(
      data16.agg(product(col("x"))),
      Row((1L to 16L).reduce { _ * _ }.toDouble)
    )

    checkAnswer(
      data16.agg(product(col("x"))),
      Row(factorials(16))
    )
  }

  test("type flexibility") {
    val bytes16 = spark.createDataset((1 to 16).map { _.toByte })(Encoders.scalaByte).toDF("x")

    val variants = Map(
      "int8" -> ByteType, "int16" -> ShortType, "int32" -> IntegerType,
      "float32" -> FloatType, "float64" -> DoubleType)

    val prods = variants.foldLeft(bytes16) { case (df, (id, typ)) =>
      df.withColumn(id, df.col("x").cast(typ))
    }.agg(
      lit(1) as "dummy",
      variants.keys.toSeq.map { id => product(col(id)) as id } : _*)

    variants.keys.foreach { typ =>
      checkAnswer(
        prods.select(typ),
        Row(factorials(16))
      )
    }
  }

  test("windowed factorials") {
    val win = Window.partitionBy(lit(1)).orderBy("x")

    val prodFactorials = data16.withColumn("f", product(col("x")).over(win)).orderBy(col("x"))

    assert(prodFactorials.count() === 16)

    checkAnswer(
      prodFactorials.limit(5),
      Seq(
        Row(1, 1.0),
        Row(2, 2.0),
        Row(3, 6.0),
        Row(4, 24.0),
        Row(5, 120.0)
      )
    )

    checkAnswer(
      prodFactorials,
      factorials.zipWithIndex.drop(1).map { case (fac, idx) =>
        Row(idx, fac)
      }
    )
  }

  test("grouped factorials") {
    val grouped = data16.groupBy((col("x") % 3) as "mod3")
      .agg(
        product(col("x")) as "product",
        product(col("x") * 0.5) as "product_scaled",
        product(col("x") * -1.0) as "product_minus")
      .orderBy("mod3")

    val expectedBase = Seq(
      (3 * 6 * 9 * 12 * 15),
      (1 * 4 * 7 * 10 * 13 * 16),
      (2 * 5 * 8 * 11 * 14))

    checkAnswer(
      grouped.select("product"),
      expectedBase.map { n => Row(n.toDouble) }
    )

    checkAnswer(
      grouped.select("product_scaled"),
      expectedBase.zip(Seq(0.03125, 0.015625, 0.03125)).map { case(a, b) => Row(a * b) }
    )

    checkAnswer(
      grouped.select("product_minus"),
      expectedBase.zip(Seq(-1.0, 1.0, -1.0)).map { case(a, b) => Row(a * b) }
    )
  }
}
