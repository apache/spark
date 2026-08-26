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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite

class AllPathsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("directed: enumerate all simple paths") {
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))).toDF("id", "name")
    val edges =
      spark
        .createDataFrame(Seq((1L, 2L, "x"), (2L, 4L, "x"), (1L, 3L, "x"), (3L, 4L, "x")))
        .toDF("src", "dst", "label")
    val g = GraphFrame(vertices, edges)

    val result =
      g.allPaths.fromExpr(col("id") === 1L).toExpr(col("id") === 4L).maxPathLength(3).run()

    val actual = result
      .collect()
      .map { row =>
        row.getAs[Seq[Long]]("path") -> row.getAs[Long]("len")
      }
      .toSet

    val expected = Set((Seq(1L, 2L, 4L), 2L), (Seq(1L, 3L, 4L), 2L))
    assert(actual === expected)
  }

  test("undirected: traverse both edge directions") {
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))).toDF("id", "name")
    val edges =
      spark
        .createDataFrame(Seq((1L, 2L, "x"), (2L, 4L, "x"), (1L, 3L, "x"), (3L, 4L, "x")))
        .toDF("src", "dst", "label")
    val g = GraphFrame(vertices, edges)

    val result = g.allPaths
      .fromExpr(col("id") === 4L)
      .toExpr(col("id") === 1L)
      .setIsDirected(false)
      .maxPathLength(3)
      .run()

    val actual = result
      .collect()
      .map { row =>
        row.getAs[Seq[Long]]("path") -> row.getAs[Long]("len")
      }
      .toSet

    val expected = Set((Seq(4L, 2L, 1L), 2L), (Seq(4L, 3L, 1L), 2L))
    assert(actual === expected)
  }

  test("edge filter excludes blocked edges") {
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))).toDF("id", "name")
    val edges = spark
      .createDataFrame(
        Seq((1L, 2L, "allowed"), (2L, 4L, "allowed"), (1L, 3L, "blocked"), (3L, 4L, "allowed")))
      .toDF("src", "dst", "label")
    val g = GraphFrame(vertices, edges)

    val result = g.allPaths
      .fromExpr("id = 1")
      .toExpr("id = 4")
      .edgeFilter(col("label") =!= "blocked")
      .maxPathLength(3)
      .run()

    val rows = result.collect()
    assert(rows.length === 1)
    assert(rows.head.getAs[Seq[Long]]("path") === Seq(1L, 2L, 4L))
    assert(rows.head.getAs[Long]("len") === 2L)
  }

  test("cycle handling keeps paths simple") {
    val vertices = spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"))).toDF("id", "name")
    val edges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L), (1L, 3L)))
      .toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = g.allPaths.fromExpr("id = 1").toExpr("id = 3").maxPathLength(4).run()

    val actualPaths =
      result.collect().map(_.getAs[scala.collection.Seq[Long]]("path").toList).toSet
    assert(actualPaths === Set(List(1L, 3L), List(1L, 2L, 3L)))
    assert(!actualPaths.contains(List(1L, 2L, 1L, 3L)))
  }

  test("schema and required arguments") {
    val vertices = spark.createDataFrame(Seq((1L, "A"), (2L, "B"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = g.allPaths.fromExpr("id = 1").toExpr("id = 2").run()

    assert(result.columns.toSeq === Seq("path", "len"))
    assert(result.schema("path").dataType.asInstanceOf[ArrayType].elementType === LongType)
    assert(result.schema("len").dataType === IntegerType)

    intercept[IllegalArgumentException] {
      g.allPaths.run()
    }
    intercept[IllegalArgumentException] {
      g.allPaths.fromExpr("id = 1").run()
    }
    intercept[IllegalArgumentException] {
      g.allPaths.toExpr("id = 2").run()
    }
    intercept[IllegalArgumentException] {
      g.allPaths.fromExpr("id = 1").toExpr("id = 2").maxPathLength(0)
    }
  }

  test("no matching path returns empty dataframe") {
    val vertices = spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result =
      g.allPaths.fromExpr(col("id") === 1L).toExpr(col("id") === 3L).maxPathLength(4).run()
    assert(result.collect().isEmpty)
  }
}
