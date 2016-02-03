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

package org.apache.spark.mllib.impl

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils


class PeriodicRDDCheckpointerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import PeriodicRDDCheckpointerSuite._

  test("Persisting") {
    var rddsToCheck = Seq.empty[RDDToCheck]

    val rdd1 = createRDD(sc)
    val checkpointer = new PeriodicRDDCheckpointer[Double](10, rdd1.sparkContext)
    checkpointer.update(rdd1)
    rddsToCheck = rddsToCheck :+ RDDToCheck(rdd1, 1)
    checkPersistence(rddsToCheck, 1)

    var iteration = 2
    while (iteration < 9) {
      val rdd = createRDD(sc)
      checkpointer.update(rdd)
      rddsToCheck = rddsToCheck :+ RDDToCheck(rdd, iteration)
      checkPersistence(rddsToCheck, iteration)
      iteration += 1
    }
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    val checkpointInterval = 2
    var rddsToCheck = Seq.empty[RDDToCheck]
    sc.setCheckpointDir(path)
    val rdd1 = createRDD(sc)
    val checkpointer = new PeriodicRDDCheckpointer[Double](checkpointInterval, rdd1.sparkContext)
    checkpointer.update(rdd1)
    rdd1.count()
    rddsToCheck = rddsToCheck :+ RDDToCheck(rdd1, 1)
    checkCheckpoint(rddsToCheck, 1, checkpointInterval)

    var iteration = 2
    while (iteration < 9) {
      val rdd = createRDD(sc)
      checkpointer.update(rdd)
      rdd.count()
      rddsToCheck = rddsToCheck :+ RDDToCheck(rdd, iteration)
      checkCheckpoint(rddsToCheck, iteration, checkpointInterval)
      iteration += 1
    }

    checkpointer.deleteAllCheckpoints()
    rddsToCheck.foreach { rdd =>
      confirmCheckpointRemoved(rdd.rdd)
    }

    Utils.deleteRecursively(tempDir)
  }
}

private object PeriodicRDDCheckpointerSuite {

  case class RDDToCheck(rdd: RDD[Double], gIndex: Int)

  def createRDD(sc: SparkContext): RDD[Double] = {
    sc.parallelize(Seq(0.0, 1.0, 2.0, 3.0))
  }

  def checkPersistence(rdds: Seq[RDDToCheck], iteration: Int): Unit = {
    rdds.foreach { g =>
      checkPersistence(g.rdd, g.gIndex, iteration)
    }
  }

  /**
   * Check storage level of rdd.
   * @param gIndex  Index of rdd in order inserted into checkpointer (from 1).
   * @param iteration  Total number of rdds inserted into checkpointer.
   */
  def checkPersistence(rdd: RDD[_], gIndex: Int, iteration: Int): Unit = {
    try {
      if (gIndex + 2 < iteration) {
        assert(rdd.getStorageLevel == StorageLevel.NONE)
      } else {
        assert(rdd.getStorageLevel != StorageLevel.NONE)
      }
    } catch {
      case _: AssertionError =>
        throw new Exception(s"PeriodicRDDCheckpointerSuite.checkPersistence failed with:\n" +
          s"\t gIndex = $gIndex\n" +
          s"\t iteration = $iteration\n" +
          s"\t rdd.getStorageLevel = ${rdd.getStorageLevel}\n")
    }
  }

  def checkCheckpoint(rdds: Seq[RDDToCheck], iteration: Int, checkpointInterval: Int): Unit = {
    rdds.reverse.foreach { g =>
      checkCheckpoint(g.rdd, g.gIndex, iteration, checkpointInterval)
    }
  }

  def confirmCheckpointRemoved(rdd: RDD[_]): Unit = {
    // Note: We cannot check rdd.isCheckpointed since that value is never updated.
    //       Instead, we check for the presence of the checkpoint files.
    //       This test should continue to work even after this rdd.isCheckpointed issue
    //       is fixed (though it can then be simplified and not look for the files).
    val fs = FileSystem.get(rdd.sparkContext.hadoopConfiguration)
    rdd.getCheckpointFile.foreach { checkpointFile =>
      assert(!fs.exists(new Path(checkpointFile)), "RDD checkpoint file should have been removed")
    }
  }

  /**
   * Check checkpointed status of rdd.
   * @param gIndex  Index of rdd in order inserted into checkpointer (from 1).
   * @param iteration  Total number of rdds inserted into checkpointer.
   */
  def checkCheckpoint(
      rdd: RDD[_],
      gIndex: Int,
      iteration: Int,
      checkpointInterval: Int): Unit = {
    try {
      if (gIndex % checkpointInterval == 0) {
        // We allow 2 checkpoint intervals since we perform an action (checkpointing a second rdd)
        // only AFTER PeriodicRDDCheckpointer decides whether to remove the previous checkpoint.
        if (iteration - 2 * checkpointInterval < gIndex && gIndex <= iteration) {
          assert(rdd.isCheckpointed, "RDD should be checkpointed")
          assert(rdd.getCheckpointFile.nonEmpty, "RDD should have 2 checkpoint files")
        } else {
          confirmCheckpointRemoved(rdd)
        }
      } else {
        // RDD should never be checkpointed
        assert(!rdd.isCheckpointed, "RDD should never have been checkpointed")
        assert(rdd.getCheckpointFile.isEmpty, "RDD should not have any checkpoint files")
      }
    } catch {
      case e: AssertionError =>
        throw new Exception(s"PeriodicRDDCheckpointerSuite.checkCheckpoint failed with:\n" +
          s"\t gIndex = $gIndex\n" +
          s"\t iteration = $iteration\n" +
          s"\t checkpointInterval = $checkpointInterval\n" +
          s"\t rdd.isCheckpointed = ${rdd.isCheckpointed}\n" +
          s"\t rdd.getCheckpointFile = ${rdd.getCheckpointFile.mkString(", ")}\n" +
          s"  AssertionError message: ${e.getMessage}")
    }
  }

}
