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

package org.apache.spark.streaming.dstream

import java.io.{IOException, ObjectInputStream}

import scala.Some
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming._
import org.apache.spark.util.{TimeStampedHashMap, Utils}

/**
 * This class represents an input stream that monitors a Hadoop-compatible filesystem for new
 * files and creates a stream out of them. The way it works as follows.
 *
 * This class remembers the information about the files selected in past batches for
 * a certain duration (say, "remember window") as shown in the figure below.
 *
 *
 * ignore threshold --->|                              |<--- current batch time
 *                      |<------ remember window ----->|
 *                      |                              |
 * --------------------------------------------------------------------------------> Time
 *
 * The trailing end of the window is the "ignore threshold" and all files whose mod time
 * are less than this threshold are assumed to have already been processed and therefore ignored.
 * Files whose mode times are within the "remember window" are checked against files that have
 * already been selected and processed. This is how new files are identified in each batch -
 * files whose mod times are greater than the ignore threshold and have not been considered
 * within the remember window.
 *
 * This makes some assumptions from the underlying file system that the system is monitoring.
 * - If a file is to be visible in the file listings, it must be visible within a certain
 *   duration of the mod time of the file. This duration is the "remember window", which is set to
 *   1 minute (see `FileInputDStream.MIN_REMEMBER_DURATION`). Otherwise, the file will not be
 *   selected as the mod time will be less than the ignore threshold when it become visible.
 * - Once a file is visible, the mod time cannot change. If it does due to appends, then the
 *   processing semantics is undefined.
 * - The time of the file system does not need to be synchronized with the time of the system
 *   running Spark Streaming. The mod time is used to ignore old files based on the threshold,
 *   and we use the mod times of selected files to define that threshold.
 */
private[streaming]
class FileInputDStream[K: ClassTag, V: ClassTag, F <: NewInputFormat[K,V] : ClassTag](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true)
  extends InputDStream[(K, V)](ssc_) {

  protected[streaming] case class SelectedFileInfo(files: Array[String], minModTime: Long)

  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  @transient private[streaming] var timeToSelectedFileInfo = new HashMap[Time, SelectedFileInfo]
  @transient private var allFoundFiles = new mutable.HashSet[String]()
  @transient private var fileToModTimes = new TimeStampedHashMap[String, Long](true)
  @transient private var lastNewFileFindingTime = 0L

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  remember(FileInputDStream.calculateRememberDuration(slideDuration))

  override def start() { }

  override def stop() { }

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds. And new files may have the same modification time as the
   * latest modification time in the previous call to this method yet was not reported in
   * the previous call.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    val selectedFileInfo = findNewFiles(validTime.milliseconds)
    logInfo(s"New files at time $validTime :\n${selectedFileInfo.files.mkString("\n")}")
    timeToSelectedFileInfo += ((validTime, selectedFileInfo))
    allFoundFiles ++= selectedFileInfo.files
    Some(filesToRDD(selectedFileInfo.files))
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldFiles = timeToSelectedFileInfo.filter(_._1 < (time - rememberDuration))
    timeToSelectedFileInfo --= oldFiles.keys
    allFoundFiles --= oldFiles.values.map { _.files }.flatten
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.files.mkString(", "))).mkString("\n"))
    // Delete file mod times that weren't accessed in the last round of getting new files
    fileToModTimes.clearOldValues(lastNewFileFindingTime - 1)
  }

  /**
   * Find new files using a custom filter which selects files whose mod time is within the
   * remember window (not before it) but have not been selected yet.
   */
  private def findNewFiles(currentTime: Long): SelectedFileInfo = {
    lastNewFileFindingTime = System.currentTimeMillis

    // Find the minimum mod time of the batches we are remembering and use that
    // the threshold time for ignoring old files
    val modTimeIgnoreThreshold = if (timeToSelectedFileInfo.nonEmpty) {
      timeToSelectedFileInfo.values.map { _.minModTime }.min
    } else {
      0
    }

    logDebug(s"Getting new files for time $currentTime with ignore time $modTimeIgnoreThreshold")
    val filter = new CustomPathFilter(modTimeIgnoreThreshold)
    val newFiles = fs.listStatus(directoryPath, filter).map(_.getPath.toString)
    val timeTaken = System.currentTimeMillis - lastNewFileFindingTime
    logInfo(s"Finding new files took $timeTaken ms")
    logDebug(s"# cached file times = ${fileToModTimes.size}")
    if (timeTaken > slideDuration.milliseconds) {
      logWarning(
        "Time taken to find new files exceeds the batch size. " +
          "Consider increasing the batch size or reducing the number of " +
          "files in the monitored directory."
      )
    }
    SelectedFileInfo(newFiles, filter.minNewFileModTime)
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    val fileRDDs = files.map(file => {
      val rdd = context.sparkContext.newAPIHadoopFile[K, V, F](file)
      if (rdd.partitions.size == 0) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    })
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    fs_
  }

  private def reset()  {
    fs_ = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    allFoundFiles = new mutable.HashSet[String]()
    generatedRDDs = new HashMap[Time, RDD[(K,V)]] ()
    timeToSelectedFileInfo = new HashMap[Time, SelectedFileInfo]
    fileToModTimes = new TimeStampedHashMap[String, Long](updateTimeStampOnGet = true)
  }

  /**
   * A custom version of the DStreamCheckpointData that stores the information about the
   * files selected in every batch. This is necessary so that the files selected for the past
   * batches (that have already been defined) can be recovered correctly upon driver failure and
   * the input data of the batches are exactly the same.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    def checkpointedFileInfo = data.asInstanceOf[HashMap[Time, SelectedFileInfo]]

    override def update(time: Time) {
      checkpointedFileInfo.clear()
      checkpointedFileInfo ++= timeToSelectedFileInfo
    }

    override def cleanup(time: Time) { }

    override def restore() {
      checkpointedFileInfo.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo(s"Restoring files for time $t - ${f.files.mkString(", ")}")
          timeToSelectedFileInfo += ((t, f))
          allFoundFiles ++= f.files
          generatedRDDs += ((t, filesToRDD(f.files)))
        }
      }
    }

    override def toString() = {
      "[\n" + checkpointedFileInfo.size + " file sets\n" +
        checkpointedFileInfo.map(p => (p._1, p._2.files.mkString(", "))).mkString("\n") + "\n]"
    }
  }

  /**
   * Custom PathFilter class to find new files that have modification time within the
   * remember window (that is mod time > ignore threshold) and have not been selected in that
   * window.
   */

  private class CustomPathFilter(modTimeIgnoreThreshold: Long) extends PathFilter {
    // Minimum of the mod times of new files found in the current interval
    var minNewFileModTime = -1L

    def accept(path: Path): Boolean = {
      try {
        val pathStr = path.toString
        if (!filter(path)) {  // Reject file if it does not satisfy filter
          logDebug(s"$pathStr rejected by filter")
          return false
        }
        // Reject file if it was considered earlier
        if (allFoundFiles.contains(pathStr)) {
          logDebug(s"$pathStr already considered")
          return false
        }
        val modTime = fileToModTimes.getOrElseUpdate(pathStr,
          fs.getFileStatus(path).getModificationTime())
        if (modTime <= modTimeIgnoreThreshold) {
          // Reject file if it was created before the ignore time
          logDebug(s"$pathStr ignored as mod time $modTime < ignore time $modTimeIgnoreThreshold")
          return false
        }
        if (minNewFileModTime < 0 || modTime < minNewFileModTime) {
          minNewFileModTime = modTime
        }
        logDebug(s"$pathStr accepted with mod time $modTime")
      } catch {
        case fnfe: java.io.FileNotFoundException =>
          logWarning("Error finding new files", fnfe)
          reset()
          return false
      }
      true
    }
  }
}

private[streaming]
object FileInputDStream {
  /**
   * Minimum duration of remembering the information of selected files. Files with mod times
   * older than this "window" of remembering will be ignored. So if new files are visible
   * within this window, then the file will get selected in the next batch.  
   */
  private val MIN_REMEMBER_DURATION = Minutes(1)

  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  /**
   * Calculate the duration to remember. This duration must be a multiple of the batch duration
   * while not being less than MIN_REMEMBER_DURATION.
   */
  def calculateRememberDuration(batchDuration: Duration): Duration = {
    val numMinBatches = math.ceil(
      MIN_REMEMBER_DURATION.milliseconds.toDouble / batchDuration.milliseconds).toLong
    Milliseconds(numMinBatches * batchDuration.milliseconds)
  }
}
