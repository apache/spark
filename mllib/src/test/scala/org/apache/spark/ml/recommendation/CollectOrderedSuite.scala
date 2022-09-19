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

package org.apache.spark.ml.recommendation

import org.apache.spark.ml.util.MLTest
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectOrdered
import org.apache.spark.sql.functions.{col, struct}

class CollectOrderedSuite extends MLTest {

  import testImplicits._

  @transient var dataFrame: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    dataFrame = Seq(
      (0, 3, 54f),
      (0, 4, 44f),
      (0, 5, 42f),
      (0, 6, 28f),
      (1, 3, 39f),
      (2, 3, 51f),
      (2, 5, 45f),
      (2, 6, 18f)
    ).toDF("user", "item", "score")
  }

  private def collect_ordered(e: Column, num: Int, reverse: Boolean): Column = {
    new Column(CollectOrdered(e.expr, num, reverse)
      .toAggregateExpression(false)
    )
  }

  test("k smallest with k < #items") {
    val k = 2
    val topK = dataFrame.groupBy("user")
      .agg(collect_ordered(col("score"), k, false))
      .as[(Int, Seq[Float])]
      .collect()

    val expected = Map(
      0 -> Array(28f, 42f),
      1 -> Array(39f),
      2 -> Array(18f, 45f)
    )
    assert(topK.size === expected.size)
    topK.foreach { case (k, v) => assert(v === expected(k)) }
  }

  test("k smallest with k > #items") {
    val k = 5
    val topK = dataFrame.groupBy("user")
      .agg(collect_ordered(col("score"), k, false))
      .as[(Int, Seq[Float])]
      .collect()

    val expected = Map(
      0 -> Array(28f, 42f, 44f, 54f),
      1 -> Array(39f),
      2 -> Array(18f, 45f, 51f)
    )
    assert(topK.size === expected.size)
    topK.foreach { case (k, v) => assert(v === expected(k)) }
  }

  test("k largest with k < #items") {
    val k = 2
    val topK = dataFrame.groupBy("user")
      .agg(collect_ordered(struct("score", "item"), k, true))
      .as[(Int, Seq[(Float, Int)])]
      .map(t => (t._1, t._2.map(p => (p._2, p._1))))
      .collect()

    val expected = Map(
      0 -> Array((3, 54f), (4, 44f)),
      1 -> Array((3, 39f)),
      2 -> Array((3, 51f), (5, 45f))
    )
    assert(topK.size === expected.size)
    topK.foreach { case (k, v) => assert(v === expected(k)) }
  }

  test("k largest with k > #items") {
    val k = 5
    val topK = dataFrame.groupBy("user")
      .agg(collect_ordered(struct("score", "item"), k, true))
      .as[(Int, Seq[(Float, Int)])]
      .map(t => (t._1, t._2.map(p => (p._2, p._1))))
      .collect()

    val expected = Map(
      0 -> Array((3, 54f), (4, 44f), (5, 42f), (6, 28f)),
      1 -> Array((3, 39f)),
      2 -> Array((3, 51f), (5, 45f), (6, 18f))
    )
    assert(topK.size === expected.size)
    topK.foreach { case (k, v) => assert(v === expected(k)) }
  }
}
