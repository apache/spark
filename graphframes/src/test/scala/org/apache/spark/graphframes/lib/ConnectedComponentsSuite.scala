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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes._
import org.apache.spark.graphframes.GraphFrame._
import org.apache.spark.graphframes.examples.Graphs

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  // vertices and edges for pruning node optimization tests.
  var verticesOpt: DataFrame = _
  var edgesOpt: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    verticesOpt = spark.range(7L).toDF(ID)
    edgesOpt = spark
      .createDataFrame(Seq((0L, 1L), (0L, 2L), (0L, 3L), (0L, 4L), (1L, 2L), (1L, 5L)))
      .toDF(SRC, DST)
  }

  test("default params") {
    val g = Graphs.empty[Int]
    val cc = g.connectedComponents
    // That is OK! It is just a name-change
    assert(cc.getAlgorithm === "two_phase")
    assert(cc.getBroadcastThreshold === 1000000)
    assert(cc.getCheckpointInterval === 2)
    assert(!cc.getUseLabelsAsComponents)
    spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
  }

  test("using labels as components") {
    spark.conf.set("spark.graphframes.useLabelsAsComponents", "true")
    val vertices =
      spark.createDataFrame(Seq("a", "b", "c", "d", "e").map(Tuple1.apply)).toDF(ID)
    val edges = spark.createDataFrame(Seq.empty[(String, String)]).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    val expected = Seq("a", "b", "c", "d", "e").map(Set(_)).toSet
    assertComponents(components, expected)
    components.unpersist()
    spark.conf.set("spark.graphframes.useLabelsAsComponents", "false")
  }

  test("don't using labels as components") {
    val vertices =
      spark.createDataFrame(Seq("a", "b", "c", "d", "e").map(Tuple1.apply)).toDF(ID)
    val edges = spark.createDataFrame(Seq.empty[(String, String)]).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    assert(components.schema("component").dataType == LongType)
    components.unpersist()
  }

  test("friends graph with different broadcast thresholds") {
    val friends = Graphs.friends
    val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))
    for ((algorithm, broadcastThreshold) <-
        Seq(
          ("graphx", 1000000),
          ("graphframes", 100000),
          ("graphframes", 1),
          ("graphframes", -1))) {
      val components = friends.connectedComponents
        .setAlgorithm(algorithm)
        .setBroadcastThreshold(broadcastThreshold)
        .run()
      assertComponents(components, expected)
      components.unpersist()
    }
  }

  Seq(true, false).foreach(useSkewedJoin => {
    Seq(true, false).foreach(useLocalCheckpoint => {
      val testPostfixName = s"${if (useLocalCheckpoint) " with local checkpoint"
        else ""}${if (useSkewedJoin) ", skewed join" else ", AQE join"}"
      val broadcastThreshold = if (useSkewedJoin) 1000000 else -1

      test(s"non trivial graph #873$testPostfixName") {
        val edges = spark
          .createDataFrame(
            Seq((1L, 2L), (2L, 3L), (3L, 4L), (4L, 5L), (5L, 1L), (6L, 7L), (7L, 8L), (8L, 6L)))
          .toDF("src", "dst")

        val vertices =
          edges.select("src").union(edges.select("dst")).distinct().withColumnRenamed("src", "id")
        val g = GraphFrame(vertices, edges)

        val result = g.connectedComponents
          .setBroadcastThreshold(broadcastThreshold)
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .run()
        val numComps = result.select("component").distinct().count()
        assert(numComps == 2L)
        result.unpersist()
      }

      test(s"empty graph$testPostfixName") {
        for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
          val components = empty.connectedComponents
            .setBroadcastThreshold(broadcastThreshold)
            .setUseLocalCheckpoints(useLocalCheckpoint)
            .run()
          assert(components.count() === 0L)
          components.unpersist()
        }
      }

      test(s"single vertex$testPostfixName") {
        val v = spark.createDataFrame(List((0L, "a", "b"))).toDF("id", "vattr", "gender")
        // Create an empty dataframe with the proper columns.
        val e = spark
          .createDataFrame(List((0L, 0L, 1L)))
          .toDF("src", "dst", "test")
          .filter("src > 10")
        val g = GraphFrame(v, e)
        val comps = g.connectedComponents
          .setBroadcastThreshold(broadcastThreshold)
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .run()
        TestUtils.testSchemaInvariants(g, comps)
        TestUtils.checkColumnType(comps.schema, "component", DataTypes.LongType)
        assert(comps.count() === 1)
        assert(
          comps.select("id", "component", "vattr", "gender").collect()
            === Seq(Row(0L, 0L, "a", "b")))
        comps.unpersist()
      }

      test(s"disconnected vertices$testPostfixName") {
        val n = 5L
        val vertices = spark.range(n).toDF(ID)
        val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF(SRC, DST)
        val g = GraphFrame(vertices, edges)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = (0L until n).map(Set(_)).toSet
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"two connected vertices$testPostfixName") {
        val v =
          spark.createDataFrame(List((0L, "a0", "b0"), (1L, "a1", "b1"))).toDF("id", "A", "B")
        val e = spark.createDataFrame(List((0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
        val g = GraphFrame(v, e)
        val comps = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        TestUtils.testSchemaInvariants(g, comps)
        assert(comps.count() === 2)
        val vxs = comps.sort("id").select("id", "component", "A", "B").collect()
        assert(List(Row(0L, 0L, "a0", "b0"), Row(1L, 0L, "a1", "b1")) === vxs)
        comps.unpersist()
      }

      test(s"chain graph$testPostfixName") {
        val n = 5L
        val g = Graphs.chain(5L)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set((0L until n).toSet)
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"star graph$testPostfixName") {
        val n = 5L
        val g = Graphs.star(5L)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set((0L to n).toSet)
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"two blobs$testPostfixName") {
        val n = 5L
        val g = Graphs.twoBlobs(n.toInt)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set((0L until 2 * n).toSet)
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"two components$testPostfixName") {
        val vertices = spark.range(6L).toDF(ID)
        val edges = spark
          .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
          .toDF(SRC, DST)
        val g = GraphFrame(vertices, edges)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L))
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"one component, differing edge directions$testPostfixName") {
        val vertices = spark.range(5L).toDF(ID)
        val edges = spark
          .createDataFrame(
            Seq(
              // 0 -> 4 -> 3 <- 2 -> 1
              (0L, 4L),
              (4L, 3L),
              (2L, 3L),
              (2L, 1L)))
          .toDF(SRC, DST)
        val g = GraphFrame(vertices, edges)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set((0L to 4L).toSet)
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"two components and two dangling vertices$testPostfixName") {
        val vertices = spark.range(8L).toDF(ID)
        val edges = spark
          .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
          .toDF(SRC, DST)
        val g = GraphFrame(vertices, edges)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L), Set(6L), Set(7L))
        assertComponents(components, expected)
        components.unpersist()
      }

      test(s"really large long IDs$testPostfixName") {
        val max = Long.MaxValue
        val chain = examples.Graphs.chain(10L)
        val vertices = chain.vertices.select((lit(max) - col(ID)).as(ID))
        val edges =
          chain.edges.select((lit(max) - col(SRC)).as(SRC), (lit(max) - col(DST)).as(DST))
        val g = GraphFrame(vertices, edges)
        val components = g.connectedComponents
          .setUseLocalCheckpoints(useLocalCheckpoint)
          .setBroadcastThreshold(broadcastThreshold)
          .run()
        assert(components.count() === 10L)
        assert(components.groupBy("component").count().count() === 1L)
        components.unpersist()
      }
    })
  })

  test("set configuration from spark conf") {
    spark.conf.set("spark.graphframes.connectedComponents.algorithm", "GRAPHX")
    assert(Graphs.friends.connectedComponents.getAlgorithm == "graphx")

    spark.conf.set("spark.graphframes.connectedComponents.broadcastthreshold", "1000")
    assert(Graphs.friends.connectedComponents.getBroadcastThreshold == 1000)

    spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", "5")
    assert(Graphs.friends.connectedComponents.getCheckpointInterval == 5)

    spark.conf
      .set("spark.graphframes.connectedComponents.intermediatestoragelevel", "memory_only")
    assert(
      Graphs.friends.connectedComponents.getIntermediateStorageLevel == StorageLevel.MEMORY_ONLY)

    spark.conf.unset("spark.graphframes.connectedComponents.algorithm")
    spark.conf.unset("spark.graphframes.connectedComponents.broadcastthreshold")
    spark.conf.unset("spark.graphframes.connectedComponents.checkpointinterval")
    spark.conf.unset("spark.graphframes.connectedComponents.intermediatestoragelevel")
  }

  Seq(StorageLevel.DISK_ONLY, StorageLevel.MEMORY_ONLY, StorageLevel.NONE).foreach(
    storageLevel => {
      test(s"intermediate storage level $storageLevel") {
        val friends = Graphs.friends
        val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))

        val components =
          friends.connectedComponents.setIntermediateStorageLevel(storageLevel).run()
        assertComponents(components, expected)
        components.unpersist()
        ()
      }
    })

  Seq(StorageLevel.DISK_ONLY, StorageLevel.MEMORY_ONLY).foreach(storageLevel => {
    test(s"intermediate storage level without skewedJoin $storageLevel") {
      val friends = Graphs.friends
      val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))

      val components =
        friends.connectedComponents
          .setIntermediateStorageLevel(storageLevel)
          .setBroadcastThreshold(-1)
          .run()
      assertComponents(components, expected)
      components.unpersist()
      ()
    }
  })

  test("not leaking cached data") {
    val priorCachedDFsSize = spark.sparkContext.getPersistentRDDs.size

    val cc = Graphs.friends.connectedComponents
    val components = cc.run()

    components.unpersist(blocking = true)

    assert(spark.sparkContext.getPersistentRDDs.size === priorCachedDFsSize)
  }

  test("prune process for pruning nodes optimization") {
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val shrinkageThreshold = 2.0
    val Some(r1) = TwoPhase.pruneLeafNodes(
      edgesOpt,
      intermediateStorageLevel,
      verticesOpt.count(),
      shrinkageThreshold)

    val expectedV = Set(Row(0L), Row(1L), Row(2L))
    val expectedE = Set(Row(0L, 1L), Row(1L, 2L), Row(0L, 2L))

    assert(r1._1.collect().toSet == expectedV)
    assert(r1._2.select(SRC, DST).collect().toSet == expectedE)
    assert(r1._3 == expectedV.size)
    r1._1.unpersist()
    r1._2.unpersist()
  }

  test("shrinkage condition for pruning nodes optimization") {
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val shrinkageThreshold = 4.0
    // new_vv_cnt = 3, nodeNum = 7, shrinkageThreshold = 4
    // new_vv_cnt * shrinkageThreshold > nodeNum. Do not perform the optimization.
    val r1 = TwoPhase.pruneLeafNodes(
      edgesOpt,
      intermediateStorageLevel,
      verticesOpt.count(),
      shrinkageThreshold)
    assert(r1 == None)
  }

  test("join back for pruning node optimization") {
    val v1 = spark.range(3L).toDF(ID)
    val e1 = spark.createDataFrame(Seq((0L, 1L), (0L, 2L))).toDF(SRC, DST)
    val r = TwoPhase.joinBack(v1, e1, edgesOpt)
    val expectedR =
      Set(Row(0L, 0L), Row(0L, 1L), Row(0L, 2L), Row(0L, 3L), Row(0L, 4L), Row(0L, 5L))
    assert(r.collect().toSet == expectedR)
  }

  private def assertComponents[T: ClassTag: TypeTag](
      actual: DataFrame,
      expected: Set[Set[T]]): Unit = {
    import actual.sparkSession.implicits._
    // note: not using agg + collect_list because collect_list is not available in 1.6.2 w/o hive
    val actualComponents = actual
      .select("component", "id")
      .as[(T, T)]
      .rdd
      .groupByKey()
      .values
      .map(_.toSeq)
      .collect()
      .map { ids =>
        val idSet = ids.toSet
        assert(
          idSet.size === ids.size,
          s"Found duplicated component assignment in [${ids.mkString(",")}].")
        idSet
      }
      .toSet
    assert(actualComponents === expected)
    ()
  }
}
