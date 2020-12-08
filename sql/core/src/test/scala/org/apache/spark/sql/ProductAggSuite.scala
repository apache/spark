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

import org.apache.spark.sql.functions.{ col, lit, product }
import org.apache.spark.sql.test.SharedSparkSession


class ProductAggSuite extends QueryTest with SharedSparkSession {
  // Sequence of integers small enough that factorial is representable exactly as DoubleType:
  private lazy val data16 = spark.range(1, 17).toDF("x")

  private lazy val factorials = (1 to 16).scanLeft(1L) { case (f, x) => f * x }

  test("bare factorial") {
    implicit val enc = Encoders.scalaDouble

    val prod = data16.agg(product("x")).as[Double].head
    val expected = (1L to 16L).reduce { _ * _ }.toDouble

    assert(prod === expected)
    assert(prod === factorials(16))
  }

  test("windowed factorials") {
    import org.apache.spark.sql.expressions.Window

    implicit val enc = Encoders.product[(Long, Double)]
    val win = Window.partitionBy(lit(1)).orderBy("x")

    val prodFactorials = data16.withColumn("f", product("x").over(win))

    val prodMap = prodFactorials.as[(Long, Double)].collect.toMap

    assert(prodMap.size === 16)

    assert(prodMap(1) === 1.0)
    assert(prodMap(2) === 2.0)
    assert(prodMap(3) === 6.0)
    assert(prodMap(4) === 24.0)
    assert(prodMap(5) === 120.0)

    factorials.zipWithIndex.drop(1).foreach { case (expected, idx) =>
      assert(prodMap(idx) === expected)
    }
  }

  test("grouped factorials") {
    implicit val enc = Encoders.scalaDouble

    val grouped = data16.groupBy((col("x") % 3) as "mod3")
                        .agg(product(col("x")) as "product",
                             product(col("x"), 0.5) as "product_scaled",
                             product(col("x"), 1.0) as "product_unity",
                             product(col("x"), -1.0) as "product_minus")
                        .orderBy("mod3")

    def col2seq(s: String): Seq[Double] =
      grouped.select(s).as[Double].collect.toSeq

    val expectedBase = Seq((3 * 6 * 9 * 12 * 15),
                           (1 * 4 * 7 * 10 * 13 * 16),
                           (2 * 5 * 8 * 11 * 14))

    assert(col2seq("product") === expectedBase.map { _.toDouble })
    assert(col2seq("product_scaled") ===
            expectedBase.zip(Seq(0.03125, 0.015625, 0.03125)).map { case(a, b) => a * b })
    assert(col2seq("product_unity") === expectedBase.map { _.toDouble })
    assert(col2seq("product_minus") ===
            expectedBase.zip(Seq(-1.0, 1.0, -1.0)).map { case(a, b) => a * b })
  }
}
