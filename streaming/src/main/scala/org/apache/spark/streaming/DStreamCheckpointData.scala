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

package org.apache.spark.streaming

import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.Logging

import java.io.{ObjectInputStream, IOException}

private[streaming]
class DStreamCheckpointData[T: ClassTag] (dstream: DStream[T])
  extends Serializable with Logging {
  protected val data = new HashMap[Time, AnyRef]()

  // Mapping of the batch time to the checkpointed RDD file of that time
  @transient private var timeToCheckpointFile = new HashMap[Time, String]
  // Mapping of the batch time to the time of the oldest checkpointed RDD in that batch's checkpoint data
  @transient private var timeToOldestCheckpointFileTime = new HashMap[Time, Time]
  @transient private var fileSystem : FileSystem = null
  protected[streaming] def currentCheckpointFiles = data.asInstanceOf[HashMap[Time, String]]

  /**
   * Updates the checkpoint data of the DStream. This gets called every time
   * the graph checkpoint is initiated. Default implementation records the
   * checkpoint files to which the generate RDDs of the DStream has been saved.
   */
  def update(time: Time) {

    // Get the checkpointed RDDs from the generated RDDs
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))

    // Add the checkpoint files to the data to be serialized 
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
      timeToCheckpointFile ++= currentCheckpointFiles
      timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
    }
  }

  /**
   * Cleanup old checkpoint data. This gets called every time the graph
   * checkpoint is initiated, but after `update` is called. Default
   * implementation, cleans up old checkpoint files.
   */
  def cleanup(time: Time) {
    timeToOldestCheckpointFileTime.remove(time) match {
      case Some(lastCheckpointFileTime) =>
        val filesToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
        logDebug("Files to delete:\n" + filesToDelete.mkString(","))
        filesToDelete.foreach {
          case (time, file) =>
            try {
              val path = new Path(file)
              if (fileSystem == null) {
                fileSystem = path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
              }
              fileSystem.delete(path, true)
              timeToCheckpointFile -= time
              logInfo("Deleted checkpoint file '" + file + "' for time " + time)
            } catch {
              case e: Exception =>
                logWarning("Error deleting old checkpoint file '" + file + "' for time " + time, e)
                fileSystem = null
            }
        }
      case None =>
        logInfo("Nothing to delete")
    }
  }

  /**
   * Restore the checkpoint data. This gets called once when the DStream graph
   * (along with its DStreams) are being restored from a graph checkpoint file.
   * Default implementation restores the RDDs from their checkpoint files.
   */
  def restore() {
    // Create RDDs from the checkpoint data
    currentCheckpointFiles.foreach {
      case(time, file) => {
        logInfo("Restoring checkpointed RDD for time " + time + " from file '" + file + "'")
        dstream.generatedRDDs += ((time, dstream.context.sparkContext.checkpointFile[T](file)))
      }
    }
  }

  override def toString() = {
    "[\n" + currentCheckpointFiles.size + " checkpoint files \n" + currentCheckpointFiles.mkString("\n") + "\n]"
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    ois.defaultReadObject()
    timeToOldestCheckpointFileTime = new HashMap[Time, Time]
    timeToCheckpointFile = new HashMap[Time, String]
  }
}
