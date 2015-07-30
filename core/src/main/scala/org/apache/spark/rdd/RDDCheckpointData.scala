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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.util.SerializableConfiguration

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * [ Initialized --> checkpointing in progress --> checkpointed ].
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with a RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 */
private[spark] class RDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends Logging with Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  private var cpState = Initialized

  // The file to which the associated RDD has been checkpointed to
  private var cpFile: Option[String] = None

  // The CheckpointRDD created from the checkpoint file, that is, the new parent the associated RDD.
  // This is defined if and only if `cpState` is `Checkpointed`.
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  // Is the RDD already checkpointed
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  // Get the file to which this RDD was checkpointed to as an Option
  def getCheckpointFile: Option[String] = RDDCheckpointData.synchronized {
    cpFile
  }

  /**
   * Materialize this RDD and write its content to a reliable DFS.
   * This is called immediately after the first action invoked on this RDD has completed.
   */
  def doCheckpoint(): Unit = {

    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    // Create the output path for the checkpoint
    val path = RDDCheckpointData.rddCheckpointDataPath(rdd.context, rdd.id).get
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {
      throw new SparkException(s"Failed to create checkpoint path $path")
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = rdd.context.broadcast(
      new SerializableConfiguration(rdd.context.hadoopConfiguration))
    val newRDD = new CheckpointRDD[T](rdd.context, path.toString)
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    rdd.context.runJob(rdd, CheckpointRDD.writeToFile[T](path.toString, broadcastedConf) _)
    if (newRDD.partitions.length != rdd.partitions.length) {
      throw new SparkException(
        "Checkpoint RDD " + newRDD + "(" + newRDD.partitions.length + ") has different " +
          "number of partitions than original RDD " + rdd + "(" + rdd.partitions.length + ")")
    }

    // Change the dependencies and partitions of the RDD
    RDDCheckpointData.synchronized {
      cpFile = Some(path.toString)
      cpRDD = Some(newRDD)
      rdd.markCheckpointed(newRDD)   // Update the RDD's dependencies and partitions
      cpState = Checkpointed
    }
    logInfo(s"Done checkpointing RDD ${rdd.id} to $path, new parent is RDD ${newRDD.id}")
  }

  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.get.partitions
  }

  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized {
    cpRDD
  }
}

private[spark] object RDDCheckpointData {

  /** Return the path of the directory to which this RDD's checkpoint data is written. */
  def rddCheckpointDataPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /** Clean up the files associated with the checkpoint data for this RDD. */
  def clearRDDCheckpointData(sc: SparkContext, rddId: Int): Unit = {
    rddCheckpointDataPath(sc, rddId).foreach { path =>
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }
  }
}
