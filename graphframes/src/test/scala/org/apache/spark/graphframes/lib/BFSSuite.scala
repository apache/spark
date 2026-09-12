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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

class BFSSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  // First graph uses String IDs
  @transient var v: DataFrame = _
  @transient var e: DataFrame = _
  @transient var g: GraphFrame = _

  // Second graph uses Int IDs
  @transient var v2: DataFrame = _
  @transient var e2: DataFrame = _
  @transient var g2: GraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    /*
      a -> b <-> c
      ^          ^
      |          |
      d <- e --> f  Also, self-edge for f
     */
    v = spark
      .createDataFrame(
        List(("a", "f"), ("b", "f"), ("c", "m"), ("d", "f"), ("e", "m"), ("f", "m")))
      .toDF("id", "gender")
    e = spark
      .createDataFrame(
        List(
          ("a", "b", "friend"),
          ("b", "c", "follow"),
          ("c", "b", "follow"),
          ("f", "c", "follow"),
          ("e", "f", "follow"),
          ("e", "d", "friend"),
          ("d", "a", "friend"),
          ("f", "f", "self")))
      .toDF("src", "dst", "relationship")
    g = GraphFrame(v, e)

    v2 =
      spark.createDataFrame(List((0L, "f"), (1L, "m"), (2L, "m"), (3L, "f"))).toDF("id", "gender")
    e2 = spark.createDataFrame(List((0L, 1L), (1L, 2L), (2L, 3L), (2L, 0L))).toDF("src", "dst")
    g2 = GraphFrame(v2, e2)
  }

  override def afterAll(): Unit = {
    v = null
    e = null
    g = null
    v2 = null
    e2 = null
    g2 = null
    super.afterAll()
  }

  test("unmatched queries should return nothing") {
    val badStart = g.bfs.fromExpr(col("id") === "howdy").toExpr(col("id") === "a").run()
    assert(badStart.count() === 0)
    val badEnd = g.bfs.fromExpr(col("id") === "a").toExpr(col("id") === "howdy").run()
    assert(badEnd.count() === 0)
  }

  test("0 hops, aka from=to") {
    val paths = g.bfs.fromExpr(col("id") === "a").toExpr(col("id") === "a").run()
    assert(paths.count() === 1)
    assert(paths.columns === Seq("from", "to"))
    assert(paths.select("from.id").head().getString(0) === "a")
    assert(paths.select("to.id").head().getString(0) === "a")
  }

  test("1 hop, aka single edge paths") {
    val paths = g.bfs.fromExpr(col("id") === "a").toExpr(col("id") === "b").run()
    assert(paths.count() === 1)
    assert(paths.columns === Seq("from", "e0", "to"))
    assert(paths.select("from.id", "to.id").head() === Row("a", "b"))
  }

  test("ties") {
    val paths = g.bfs.fromExpr(col("id") === "e").toExpr(col("id") === "b").run()
    assert(paths.count() === 2)
    assert(paths.columns === Seq("from", "e0", "v1", "e1", "v2", "e2", "to"))
    paths.select("to.id").collect().foreach {
      case Row(id: String) =>
        assert(id === "b")
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("maxPathLength: length 1") {
    val paths = g.bfs.fromExpr(col("id") === "e").toExpr(col("id") === "f").maxPathLength(1).run()
    assert(paths.count() === 1)
    val paths0 =
      g.bfs.fromExpr(col("id") === "e").toExpr(col("id") === "f").maxPathLength(0).run()
    assert(paths0.count() === 0)
  }

  test("maxPathLength: length > 1") {
    val paths = g.bfs.fromExpr(col("id") === "e").toExpr(col("id") === "b").maxPathLength(3).run()
    assert(paths.count() === 2)
    val paths0 =
      g.bfs.fromExpr(col("id") === "e").toExpr(col("id") === "b").maxPathLength(2).run()
    assert(paths0.count() === 0)
  }

  test("edge filter") {
    val paths1 = g.bfs
      .fromExpr(col("id") === "e")
      .toExpr(col("id") === "b")
      .edgeFilter(col("src") =!= "d")
      .run()
    assert(paths1.count() === 1)
    paths1.select("e0.dst").collect().foreach {
      case Row(id: String) =>
        assert(id === "f")
      case _: Row => throw new GraphFramesUnreachableException()
    }
    val paths2 = g.bfs
      .fromExpr(col("id") === "e")
      .toExpr(col("id") === "b")
      .edgeFilter(col("relationship") === "friend")
      .run()
    assert(paths2.count() === 1)
    paths2.select("e0.dst").collect().foreach {
      case Row(id: String) =>
        assert(id === "d")
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("string expressions") {
    val paths1 = g.bfs
      .fromExpr("id = 'e'")
      .toExpr("id = 'b'")
      .edgeFilter("src != 'd'")
      .run()
    assert(paths1.count() === 1)
    paths1.select("e0.dst").collect().foreach {
      case Row(id: String) =>
        assert(id === "f")
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("fromExpr and toExpr are required") {
    intercept[IllegalArgumentException] {
      g.bfs.run()
    }
    intercept[IllegalArgumentException] {
      g.bfs.fromExpr("id = 'e'").run()
    }
    intercept[IllegalArgumentException] {
      g.bfs.toExpr("id = 'b'").run()
    }
  }
}
