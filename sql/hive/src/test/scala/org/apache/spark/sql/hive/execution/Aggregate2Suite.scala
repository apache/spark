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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.{QueryTest, Row}
import org.scalatest.BeforeAndAfterAll
import test.org.apache.spark.sql.hive.aggregate2.MyJavaUDAF

class Aggregate2Suite extends QueryTest with BeforeAndAfterAll {

  protected lazy val ctx = TestHive
  import ctx.implicits._

  var originalUseAggregate2: Boolean = _

  override def beforeAll(): Unit = {
    originalUseAggregate2 = ctx.conf.useSqlAggregate2
    ctx.sql("set spark.sql.useAggregate2=true")
    val data = Seq[(Integer, Integer)](
      (1, 10),
      (null, -60),
      (1, 20),
      (1, 30),
      (2, 0),
      (null, -10),
      (2, -1),
      (2, null),
      (2, null),
      (null, 100),
      (3, null),
      (null, null),
      (3, null)).toDF("key", "value")

    data.write.saveAsTable("agg2")
  }

  test("test average2 no key in output") {
    checkAnswer(
      ctx.sql(
        """
          |SELECT avg(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(-0.5) :: Row(20.0) :: Row(null) :: Row(10.0) :: Nil)
  }

  test("test average2") {
    ctx.sql(
      """
        |SELECT key, avg(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).explain(true)

    ctx.sql(
      """
        |SELECT key, avg(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).collect().foreach(println)

    checkAnswer(
      ctx.sql(
        """
          |SELECT key, avg(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(1, 20.0) :: Row(2, -0.5) :: Row(3, null) :: Row(null, 10.0) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT avg(value), key
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(20.0, 1) :: Row(-0.5, 2) :: Row(null, 3) :: Row(10.0, null) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT avg(value) + 1.5, key + 10
          |FROM agg2
          |GROUP BY key + 10
        """.stripMargin),
      Row(21.5, 11) :: Row(1.0, 12) :: Row(null, 13) :: Row(11.5, null) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT avg(value) FROM agg2
        """.stripMargin),
      Row(11.125) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT avg(null)
        """.stripMargin),
      Row(null) :: Nil)

  }

  test("non-AlgebraicAggregate aggreguate function") {
    ctx.sql(
      """
        |SELECT key, mydoublesum(cast(value as double))
        |FROM agg2
        |GROUP BY key
      """.stripMargin).explain(true)

    ctx.sql(
      """
        |SELECT key, mydoublesum(cast(value as double))
        |FROM agg2
        |GROUP BY key
      """.stripMargin).collect().foreach(println)

    checkAnswer(
      ctx.sql(
        """
          |SELECT mydoublesum(cast(value as double)), key
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1) :: Row(-1.0, 2) :: Row(null, 3) :: Row(30.0, null) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT mydoublesum(cast(value as double)) FROM agg2
        """.stripMargin),
      Row(89.0) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT mydoublesum(null)
        """.stripMargin),
      Row(null) :: Nil)

  }

  test("non-AlgebraicAggregate and AlgebraicAggregate aggreguate function") {
    checkAnswer(
      ctx.sql(
        """
          |SELECT mydoublesum(cast(value as double)), key, avg(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1, 20.0) ::
        Row(-1.0, 2, -0.5) ::
        Row(null, 3, null) ::
        Row(30.0, null, 10.0) :: Nil)

    checkAnswer(
      ctx.sql(
        """
          |SELECT
          |  mydoublesum(cast(value as double) + 1.5 * key),
          |  avg(value - key),
          |  key,
          |  mydoublesum(cast(value as double) - 1.5 * key),
          |  avg(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(64.5, 19.0, 1, 55.5, 20.0) ::
        Row(5.0, -2.5, 2, -7.0, -0.5) ::
        Row(null, null, 3, null, null) ::
        Row(null, null, null, null, 10.0) :: Nil)
  }

  test("udaf") {
    val myJavaUDAF = new MyJavaUDAF
    ctx.udaf.register("myjavaudaf", myJavaUDAF)

    ctx.sql(
      """
        |SELECT
        |  key,
        |  mydoublesum(cast(value as double) + 1.5 * key),
        |  avg(value - key),
        |  myjavaudaf(value),
        |  mydoublesum(cast(value as double) - 1.5 * key),
        |  avg(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).explain(true)

    ctx.sql(
      """
        |SELECT
        |  key,
        |  mydoublesum(cast(value as double) + 1.5 * key),
        |  avg(value - key),
        |  myjavaudaf(value),
        |  mydoublesum(cast(value as double) - 1.5 * key),
        |  avg(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).collect().foreach(println)

    checkAnswer(
      ctx.sql(
        """
          |SELECT
          |  key,
          |  mydoublesum(cast(value as double) + 1.5 * key),
          |  avg(value - key),
          |  myjavaudaf(value),
          |  mydoublesum(cast(value as double) - 1.5 * key),
          |  avg(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(1, 64.5, 19.0, 6000.0, 55.5, 20.0) ::
        Row(2, 5.0, -2.5, -0.0, -7.0, -0.5) ::
        Row(3, null, null, null, null, null) ::
        Row(null, null, null, 60000.0, null, 10.0) :: Nil)
  }

  override def afterAll(): Unit = {
    ctx.sql(s"set spark.sql.useAggregate2=$originalUseAggregate2")
  }
}
