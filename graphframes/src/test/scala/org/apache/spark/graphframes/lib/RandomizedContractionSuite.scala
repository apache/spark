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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.examples.Graphs

class RandomizedContractionSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("RandomizedContraction: empty graph") {
    val graph = Graphs.empty[Long]
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 0L)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: single isolated vertex") {
    val vertices = spark.createDataFrame(List((0L, "a", "b"))).toDF("id", "vattr", "gender")
    val e =
      spark.createDataFrame(List((0L, 0L, 1L))).toDF("src", "dst", "test").filter("src > 10")
    val graph = GraphFrame(vertices, e)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 1L)
    assert(components.select("id", "component").collect().toSet === Set(Row(0L, 0L)))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: two connected vertices") {
    val vertices =
      spark.createDataFrame(List((0L, "a0", "b0"), (1L, "a1", "b1"))).toDF("id", "A", "B")
    val edges = spark.createDataFrame(List((0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 2L)
    val compValues = components.select("id", "component").collect()
    assert(compValues.map(_.getLong(1)).toSet.size === 1)
    assert(compValues.map(_.getLong(0)) === Array(0L, 1L))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: chain graph") {
    val n = 5L
    val graph = Graphs.chain(n)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === n)
    assert(components.select("component").distinct().count() === 1L)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: disconnected vertices") {
    val n = 5L
    val vertices = spark.range(n).toDF(GraphFrame.ID)
    val edges =
      spark.createDataFrame(Seq.empty[(Long, Long)]).toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === n)
    assert(components.select("component").distinct().count() === n)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: two separate components") {
    val vertices = spark.range(6L).toDF(GraphFrame.ID)
    val edges = spark
      .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 6L)
    val compGroups =
      components.groupBy("component").count().collect().map(r => r.getLong(1)).toSet
    assert(compGroups === Set(3L, 3L))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: with dangling vertices") {
    val vertices = spark.range(8L).toDF(GraphFrame.ID)
    val edges = spark
      .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 8L)
    val compCounts =
      components.groupBy("component").count().collect().map(r => r.getLong(1)).toSet
    assert(compCounts === Set(1L, 1L, 3L, 3L))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: useLabelsAsComponents with string IDs") {
    val vertices =
      spark.createDataFrame(Seq("a", "b", "c", "d").map(Tuple1.apply)).toDF(GraphFrame.ID)
    val edges =
      spark.createDataFrame(Seq(("a", "b"), ("b", "c"))).toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = true,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 4L)
    val compIds = components.select("component").collect().map(_.getString(0)).toSet
    assert(compIds.size === 2)
    assert(compIds == Set("a", "d"))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: useLabelsAsComponents with long IDs") {
    val vertices =
      spark.createDataFrame(Seq(1L, 2L, 3L, 4L).map(Tuple1.apply)).toDF(GraphFrame.ID)
    val edges =
      spark.createDataFrame(Seq((1L, 2L), (2L, 3L))).toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = true,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 4L)
    val compIds = components.select("component").collect().map(_.getLong(0)).toSet
    assert(compIds.size === 2)
    assert(compIds == Set(1L, 4L))
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: no parquet file leaks") {
    val graph = Graphs.chain(3L)
    val initialParquetFiles = listParquetFiles()

    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    components.count()

    val finalParquetFiles = listParquetFiles()
    assert(finalParquetFiles === initialParquetFiles)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: no memory leaks") {
    val priorCached = spark.sparkContext.getPersistentRDDs

    val graph = Graphs.chain(10L)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = false,
      checkpointInterval = 1,
      isGraphPrepared = false)
    components.count()
    components.unpersist()

    val postCached = spark.sparkContext.getPersistentRDDs
    assert(postCached.size === priorCached.size)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: large long IDs") {
    val max = Long.MaxValue
    val chain = Graphs.chain(10L)
    val vertices = chain.vertices.select((col(GraphFrame.ID) - lit(max)).as(GraphFrame.ID))
    val edges = chain.edges.select(
      (col(GraphFrame.SRC) - lit(max)).as(GraphFrame.SRC),
      (col(GraphFrame.DST) - lit(max)).as(GraphFrame.DST))
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 10L)
    assert(components.select("component").distinct().count() === 1L)
    assertFunctionRegistryClean()
  }

  test("RandomizedContraction: directed edges still produce connected components") {
    val vertices = spark.range(5L).toDF(GraphFrame.ID)
    val edges = spark
      .createDataFrame(Seq((0L, 4L), (4L, 3L), (2L, 3L), (2L, 1L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val graph = GraphFrame(vertices, edges)
    val components = RandomizedContraction.run(
      inputGraph = graph,
      useLabelsAsComponents = false,
      intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK,
      useLocalCheckpoints = true,
      checkpointInterval = 1,
      isGraphPrepared = false)
    assert(components.count() === 5L)
    assert(components.select("component").distinct().count() === 1L)
    assertFunctionRegistryClean()
  }

  private def assertFunctionRegistryClean(): Unit = {
    val functionRegistry = spark.sessionState.functionRegistry
    val identifier = new FunctionIdentifier("_axpb", Some("builtin"), Some("system"))
    val _ = assert(!functionRegistry.functionExists(identifier))
  }

  private def listParquetFiles(): Set[String] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val rootPath = new org.apache.hadoop.fs.Path(spark.conf.get("spark.sql.warehouse.dir"))

    def listFiles(path: org.apache.hadoop.fs.Path): Set[String] = {
      if (fs.exists(path)) {
        fs.listStatus(path)
          .flatMap { status =>
            if (status.isDirectory) listFiles(status.getPath)
            else if (status.getPath.getName.endsWith(".parquet")) Set(status.getPath.toString)
            else Set.empty[String]
          }
          .toSet
      } else Set.empty[String]
    }

    listFiles(rootPath)
  }
}
