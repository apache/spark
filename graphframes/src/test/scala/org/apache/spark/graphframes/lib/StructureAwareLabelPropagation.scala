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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils

class StructureAwareLabelPropagationSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("basic flow: one iteration propagates strongest incoming label") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .run()

    TestUtils.testSchemaInvariants(g, result)
    TestUtils.checkColumnType(result.schema, "label", DataTypes.LongType)

    val labels = result
      .select("id", "label")
      .collect()
      .map { row =>
        row.getLong(0) -> row.getLong(1)
      }
      .toMap

    assert(labels === Map(1L -> 3L, 2L -> 1L, 3L -> 2L))

    result.unpersist()
  }

  test(
    "different structuralSimilarityMultiplier values can change winner between direct-link mass and common-neighbor overlap") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4 when direct links are included:
    // - src 1 has overlap 2 with dst 4 via neighbors {7, 8}
    // - src 2 and src 3 each have overlap 0 with dst 4
    // Messages are aggregated by label:
    //   label A total = 1 + 2m
    //   label B total = 1 + 1 = 2
    // where m = structuralSimilarityMultiplier.
    // So smaller m favors B; larger m favors A.
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val lowMultiplier = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.1)
      .run()

    val highMultiplier = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(1.0)
      .run()

    TestUtils.checkColumnType(lowMultiplier.schema, "label", DataTypes.StringType)
    TestUtils.checkColumnType(highMultiplier.schema, "label", DataTypes.StringType)

    val labelWithLowMultiplier =
      lowMultiplier.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithHighMultiplier =
      highMultiplier.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithLowMultiplier === "B")
    assert(labelWithHighMultiplier === "A")

    lowMultiplier.unpersist()
    highMultiplier.unpersist()
  }

  test("isolated vertex keeps its own ID label") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 99L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new StructureAwareLabelPropagation(g)
      .maxIter(3)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .run()

    assert(result.count() == 3)
    val isolatedLabel =
      result.filter(col("id") === lit(99L)).select("label").collect().head.getLong(0)
    assert(isolatedLabel === 99L)

    result.unpersist()
  }

  test("disconnected graph propagates labels independently per component") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L, 10L, 11L, 12L).map(Tuple1(_))).toDF("id")
    val edges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 3L), (10L, 11L), (11L, 12L)))
      .toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .run()

    val labels = result
      .select("id", "label")
      .collect()
      .map { row =>
        row.getLong(0) -> row.getLong(1)
      }
      .toMap

    assert(labels === Map(1L -> 1L, 2L -> 1L, 3L -> 2L, 10L -> 10L, 11L -> 10L, 12L -> 11L))

    result.unpersist()
  }

  test("changing only structuralSimilarityMultiplier can flip the winning label") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4 when direct links are included:
    //   label A score = 1 + 2m   (from src 1)
    //   label B score = 2        (from src 2 and src 3)
    // Keep direct-link handling fixed and vary only m.
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val lowMultiplier = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.1)
      .run()

    val highMultiplier = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(1.0)
      .run()

    val labelWithLowMultiplier =
      lowMultiplier.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithHighMultiplier =
      highMultiplier.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithLowMultiplier === "B")
    assert(labelWithHighMultiplier === "A")

    lowMultiplier.unpersist()
    highMultiplier.unpersist()
  }

  test("changing ignoreDirectLinks can flip the winning label") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4 with m = 0.25:
    // ignoreDirectLinks = false:
    //   label A score = 1 + 2m = 1.5
    //   label B score = 2
    // ignoreDirectLinks = true:
    //   label A score = 2m = 0.5
    //   label B score = 0
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val includeDirectLinks = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.25)
      .run()

    val ignoreDirectLinks = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setIgnoreDirectLinks(true)
      .setStructuralSimilarityMultiplier(0.25)
      .run()

    val labelWithDirectLinks =
      includeDirectLinks.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithoutDirectLinks =
      ignoreDirectLinks.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithDirectLinks === "B")
    assert(labelWithoutDirectLinks === "A")

    includeDirectLinks.unpersist()
    ignoreDirectLinks.unpersist()
  }

  test("setStructuralSimilarityMultiplier allows zero but rejects negative values") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    new StructureAwareLabelPropagation(g).setStructuralSimilarityMultiplier(0.0)

    intercept[IllegalArgumentException] {
      new StructureAwareLabelPropagation(g).setStructuralSimilarityMultiplier(-0.1)
    }
  }

  test("zero structuralSimilarityMultiplier is invalid when ignoreDirectLinks is true") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    intercept[IllegalArgumentException] {
      new StructureAwareLabelPropagation(g)
        .maxIter(1)
        .setIgnoreDirectLinks(true)
        .setStructuralSimilarityMultiplier(0.0)
        .run()
    }
  }

  test("setIsDirected(false) changes propagation by adding reverse links") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val directed = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .setIsDirected(true)
      .run()

    val undirected = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .setIsDirected(false)
      .run()

    val directedLabels = directed
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap
    val undirectedLabels = undirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap

    assert(directedLabels === Map(1L -> 1L, 2L -> 1L))
    assert(undirectedLabels === Map(1L -> 2L, 2L -> 1L))

    directed.unpersist()
    undirected.unpersist()
  }

  test("undirected mode matches explicitly symmetrized directed edge set") {
    assume(TestUtils.requireSparkVersionGE(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L, 4L).map(Tuple1(_))).toDF("id")
    val directedEdges =
      spark.createDataFrame(Seq((1L, 2L), (2L, 3L), (2L, 4L))).toDF("src", "dst")
    val symmetrizedEdges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L), (3L, 2L), (2L, 4L), (4L, 2L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, directedEdges)
    val gSym = GraphFrame(vertices, symmetrizedEdges)

    val internalUndirected = new StructureAwareLabelPropagation(g)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .setIsDirected(false)
      .run()

    val explicitSymDirected = new StructureAwareLabelPropagation(gSym)
      .maxIter(1)
      .setIgnoreDirectLinks(false)
      .setStructuralSimilarityMultiplier(0.5)
      .setIsDirected(true)
      .run()

    val internalMap = internalUndirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap
    val explicitMap = explicitSymDirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap

    assert(internalMap === explicitMap)

    internalUndirected.unpersist()
    explicitSymDirected.unpersist()
  }
}
