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

package org.apache.spark.graphframes.rw

import org.apache.hadoop.fs.Path

import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.examples.Graphs
import org.apache.spark.sql.functions.array_size
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType

class RandomWalkWithRestartSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("test RW base") {
    val g = Graphs.friends

    val numBatches = 5
    val rwRunner = new RandomWalkWithRestart()
      .onGraph(g)
      .setRestartProbability(0.2)
      .setGlobalSeed(42L)
      .setBatchSize(5)
      .setNumBatches(numBatches)
      .setNumWalksPerNode(10)
      .setTemporaryPrefix("/tmp")

    val runId = rwRunner.getRunId()
    try {
      val walks = rwRunner.run()

      assert(walks.schema.fields.length === 2)
      // friends has string as ID type
      assert(walks.schema(RandomWalkBase.rwColName).dataType === ArrayType(StringType))

      // num rows should be:
      // - 10 walks for each vertex that has edge
      // - vertex "g" is isolated
      // - total vertices 7
      // - total walks (7 - 1) * 10 = 60
      assert(walks.count() === 60)

      // each walk should have length numBatches * batchSize = 25
      assert(walks.filter(array_size(col(RandomWalkBase.rwColName)) =!= lit(25)).count() === 0)

      // all walk IDs are unique
      assert(walks.select(col(RandomWalkBase.walkIdCol)).distinct().count() === 60)
    } finally {
      // Clean up temporary files after the test
      rwRunner.cleanUp()

      // Verify that all temporary files have been deleted
      // scalastyle:off hadoopconfiguration
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // scalastyle:on hadoopconfiguration
      val basePath = "/tmp"
      val fs = new Path(basePath).getFileSystem(hadoopConf)
      val runPath = s"$basePath/${runId}_batch_"
      (1 to numBatches).foreach { i =>
        val path = new Path(s"${runPath}${i}")
        assert(!fs.exists(path), s"Temporary file not deleted: $path")
      }
    }
  }

  test("test RW with restart from middle iteration") {
    val g = Graphs.friends

    val numBatches = 6
    val batchSize = 5
    val totalSteps = numBatches * batchSize

    // First run: generate all 6 batches
    val rwRunner1 = new RandomWalkWithRestart()
      .onGraph(g)
      .setRestartProbability(0.2)
      .setGlobalSeed(42L)
      .setBatchSize(batchSize)
      .setNumBatches(numBatches)
      .setNumWalksPerNode(10)
      .setTemporaryPrefix("/tmp")

    val runId = rwRunner1.getRunId()
    logInfo(s"Using runId: $runId")

    // Run and persist the result
    val walks1 = rwRunner1.run()
    // Materialize and persist
    walks1.persist()
    walks1.count() // Force materialization

    // Verify walks1 properties
    assert(walks1.schema.fields.length === 2)
    assert(walks1.schema(RandomWalkBase.rwColName).dataType === ArrayType(StringType))
    assert(walks1.count() === 60) // (7-1)*10 = 60 walks
    assert(
      walks1.filter(array_size(col(RandomWalkBase.rwColName)) =!= lit(totalSteps)).count() === 0)

    // Second run: same runID, start from iteration 3
    val rwRunner2 = new RandomWalkWithRestart()
      .onGraph(g)
      .setRestartProbability(0.2)
      .setGlobalSeed(42L)
      .setBatchSize(batchSize)
      .setNumBatches(numBatches)
      .setNumWalksPerNode(10)
      .setTemporaryPrefix("/tmp")
      .setRunId(runId)
      .setStartingFromBatch(3)

    val walks2 = rwRunner2.run()
    walks2.persist()
    walks2.count() // Force materialization

    // Verify walks2 properties
    assert(walks2.schema.fields.length === 2)
    assert(walks2.schema(RandomWalkBase.rwColName).dataType === ArrayType(StringType))
    assert(walks2.count() === 60)
    assert(
      walks2.filter(array_size(col(RandomWalkBase.rwColName)) =!= lit(totalSteps)).count() === 0)

    // Compare results: they should be identical
    // Since walk IDs are UUIDs generated during the first batch, they will be different
    // between runs. So we need to compare the walks without considering walk IDs.
    // We'll sort both DataFrames by the walk array and compare the arrays directly.

    // Extract just the walk arrays and sort them
    val walks1Sorted = walks1
      .select(col(RandomWalkBase.rwColName))
      .orderBy(col(RandomWalkBase.rwColName).asc)
      .collect()
      .map(_.getSeq[String](0))

    val walks2Sorted = walks2
      .select(col(RandomWalkBase.rwColName))
      .orderBy(col(RandomWalkBase.rwColName).asc)
      .collect()
      .map(_.getSeq[String](0))

    // Both should have the same number of walks
    assert(walks1Sorted.length === walks2Sorted.length)

    // Compare each walk array
    walks1Sorted.zip(walks2Sorted).foreach { case (walk1, walk2) =>
      assert(walk1 === walk2, "Walk sequences should be identical")
    }

    // Clean up temporary files
    rwRunner2.cleanUp()

    // Verify cleanup
    // scalastyle:off hadoopconfiguration
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
    val basePath = "/tmp"
    val fs = new Path(basePath).getFileSystem(hadoopConf)
    val runPath = s"$basePath/${runId}_batch_"
    (1 to numBatches).foreach { i =>
      val path = new Path(s"${runPath}${i}")
      assert(!fs.exists(path), s"Temporary file not deleted: $path")
    }

    // Unpersist
    walks1.unpersist()
    walks2.unpersist()
  }
}
