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

import java.io.{FileNotFoundException, IOException, ObjectInputStream}

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * This class represents an input stream that monitors a Hadoop-compatible filesystem for new
 * files and creates a stream out of them. The way it works as follows.
 *
 * At each batch interval, the file system is queried for files in the given directory and
 * detected new files are selected for that batch. In this case "new" means files that
 * became visible to readers during that time period. Some extra care is needed to deal
 * with the fact that files may become visible after they are created. For this purpose, this
 * class remembers the information about the files selected in past batches for
 * a certain duration (say, "remember window") as shown in the figure below.
 *
 * {{{
 *                      |<----- remember window ----->|
 * ignore threshold --->|                             |<--- current batch time
 *                      |____.____.____.____.____.____|
 *                      |    |    |    |    |    |    |
 * ---------------------|----|----|----|----|----|----|-----------------------> Time
 *                      |____|____|____|____|____|____|
 *                             remembered batches
 * }}}
 *
 * The trailing end of the window is the "ignore threshold" and all files whose mod times
 * are less than this threshold are assumed to have already been selected and are therefore
 * ignored. Files whose mod times are within the "remember window" are checked against files
 * that have already been selected. At a high level, this is how new files are identified in
 * each batch - files whose mod times are greater than the ignore threshold and
 * have not been considered within the remember window. See the documentation on the method
 * `isNewFile` for more details.
 *
 * This makes some assumptions from the underlying file system that the system is monitoring.
 *
 *  - The clock of the file system is assumed to synchronized with the clock of the machine running
 *    the streaming app.
 *  - If a file is to be visible in the directory listings, it must be visible within a certain
 *    duration of the mod time of the file. This duration is the "remember window", which is set to
 *    1 minute (see `FileInputDStream.minRememberDuration`). Otherwise, the file will never be
 *    selected as the mod time will be less than the ignore threshold when it becomes visible.
 *  - Once a file is visible, the mod time cannot change. If it does due to appends, then the
 *    processing semantics are undefined.
 */
private[streaming]
class FileInputDStream[K, V, F <: NewInputFormat[K, V]](
    _ssc: StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true,
    conf: Option[Configuration] = None)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
  extends InputDStream[(K, V)](_ssc) {

  private val serializableConfOpt = conf.map(new SerializableConfiguration(_))

  /**
   * Minimum duration of remembering the information of selected files. Defaults to 60 seconds.
   *
   * Files with mod times older than this "window" of remembering will be ignored. So if new
   * files are visible within this window, then the file will get selected in the next batch.
   */
  private val minRememberDurationS = {
    Seconds(ssc.conf.getTimeAsSeconds("spark.streaming.fileStream.minRememberDuration",
      ssc.conf.get("spark.streaming.minRememberDuration", "60s")))
  }

  // This is a def so that it works during checkpoint recovery:
  private def clock = ssc.scheduler.clock

  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = if (newFilesOnly) clock.getTimeMillis() else 0L

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  private val numBatchesToRemember = FileInputDStream
    .calculateNumBatchesToRemember(slideDuration, minRememberDurationS)
  private val durationToRemember = slideDuration * numBatchesToRemember
  remember(durationToRemember)

  // Map of batch-time to selected file info for the remembered batches
  // This is a concurrent map because it's also accessed in unit tests
  @transient private[streaming] var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[String]]

  // Set of files that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  @transient private var _path: Path = null
  @transient private var _fs: FileSystem = null

  override def start(): Unit = { }

  override def stop(): Unit = { }

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds in HDFS. And new files may have the same modification time as the
   * latest modification time in the previous call to this method yet was not reported in
   * the previous call.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    // Find new files
    val newFiles = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    batchTimeToSelectedFiles.synchronized {
      batchTimeToSelectedFiles += ((validTime, newFiles))
    }
    recentlySelectedFiles ++= newFiles
    val rdds = Some(filesToRDD(newFiles))
    // Copy newFiles to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "files" -> newFiles.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
    val inputInfo = StreamInputInfo(id, 0, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    rdds
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time): Unit = {
    super.clearMetadata(time)
    batchTimeToSelectedFiles.synchronized {
      val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
      batchTimeToSelectedFiles --= oldFiles.keys
      recentlySelectedFiles --= oldFiles.values.flatten
      logInfo("Cleared " + oldFiles.size + " old files that were older than " +
        (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
      logDebug("Cleared files are:\n" +
        oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    }
  }

  /**
   * Find new files for the batch of `currentTime`. This is done by first calculating the
   * ignore threshold for file mod times, and then getting a list of files filtered based on
   * the current batch time and the ignore threshold. The ignore threshold is the max of
   * initial ignore threshold and the trailing end of the remember window (that is, which ever
   * is later in time).
   */
  private def findNewFiles(currentTime: Long): Array[String] = {
    try {
      lastNewFileFindingTime = clock.getTimeMillis()

      // Calculate ignore threshold
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold,   // initial threshold based on newFilesOnly setting
        currentTime - durationToRemember.milliseconds  // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")

      val directories = Option(fs.globStatus(directoryPath)).getOrElse(Array.empty[FileStatus])
        .filter(_.isDirectory)
        .map(_.getPath)
      val newFiles = directories.flatMap(dir =>
        fs.listStatus(dir)
          .filter(isNewFile(_, currentTime, modTimeIgnoreThreshold))
          .map(_.getPath.toString))
      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime
      logDebug(s"Finding new files took $timeTaken ms")
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          s"Time taken to find new files $timeTaken exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directories."
        )
      }
      newFiles
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"No directory to scan: $directoryPath: $e")
        Array.empty
      case e: Exception =>
        logWarning(s"Error finding new files under $directoryPath", e)
        reset()
        Array.empty
    }
  }

  /**
   * Identify whether the given `path` is a new file for the batch of `currentTime`. For it to be
   * accepted, it has to pass the following criteria.
   * - It must pass the user-provided file filter.
   * - It must be newer than the ignore threshold. It is assumed that files older than the ignore
   *   threshold have already been considered or are existing files before start
   *   (when newFilesOnly = true).
   * - It must not be present in the recently selected files that this class remembers.
   * - It must not be newer than the time of the batch (i.e. `currentTime` for which this
   *   file is being tested. This can occur if the driver was recovered, and the missing batches
   *   (during downtime) are being generated. In that case, a batch of time T may be generated
   *   at time T+x. Say x = 5. If that batch T contains file of mod time T+5, then bad things can
   *   happen. Let's say the selected files are remembered for 60 seconds.  At time t+61,
   *   the batch of time t is forgotten, and the ignore threshold is still T+1.
   *   The files with mod time T+5 are not remembered and cannot be ignored (since, t+5 > t+1).
   *   Hence they can get selected as new files again. To prevent this, files whose mod time is more
   *   than current batch time are not considered.
   * @param fileStatus file status
   * @param currentTime time of the batch
   * @param modTimeIgnoreThreshold the ignore threshold
   * @return true if the file has been modified within the batch window
   */
  private def isNewFile(
     fileStatus: FileStatus,
     currentTime: Long,
     modTimeIgnoreThreshold: Long): Boolean = {
    val path = fileStatus.getPath
    val pathStr = path.toString
    // Reject file if it does not satisfy filter
    if (!filter(path)) {
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    // Reject file if it was created before the ignore time
    val modTime = fileStatus.getModificationTime()
    if (modTime <= modTimeIgnoreThreshold) {
      // Use <= instead of < to avoid SPARK-4518
      logDebug(s"$pathStr ignored as mod time $modTime <= ignore time $modTimeIgnoreThreshold")
      return false
    }
    // Reject file if mod time > current batch time
    if (modTime > currentTime) {
      logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
      return false
    }
    // Reject file if it was considered earlier
    if (recentlySelectedFiles.contains(pathStr)) {
      logDebug(s"$pathStr already considered")
      return false
    }
    logDebug(s"$pathStr accepted with mod time $modTime")
    true
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    val fileRDDs = files.map { file =>
      val rdd = serializableConfOpt.map(_.value) match {
        case Some(config) => context.sparkContext.newAPIHadoopFile(
          file,
          fm.runtimeClass.asInstanceOf[Class[F]],
          km.runtimeClass.asInstanceOf[Class[K]],
          vm.runtimeClass.asInstanceOf[Class[V]],
          config)
        case None => context.sparkContext.newAPIHadoopFile[K, V, F](file)
      }
      if (rdd.partitions.isEmpty) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    }
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  private def directoryPath: Path = {
    if (_path == null) _path = new Path(directory)
    _path
  }

  private def fs: FileSystem = {
    if (_fs == null) _fs = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    _fs
  }

  private def reset(): Unit = {
    _fs = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(K, V)]]()
    batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[String]]
    recentlySelectedFiles = new mutable.HashSet[String]()
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    private def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]

    override def update(time: Time): Unit = {
      hadoopFiles.clear()
      batchTimeToSelectedFiles.synchronized { hadoopFiles ++= batchTimeToSelectedFiles }
    }

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) =>
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          batchTimeToSelectedFiles.synchronized { batchTimeToSelectedFiles += ((t, f)) }
          recentlySelectedFiles ++= f
          generatedRDDs += ((t, filesToRDD(f)))
      }
    }

    override def toString: String = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }
}

private[streaming]
object FileInputDStream {

  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  /**
   * Calculate the number of last batches to remember, such that all the files selected in
   * at least last minRememberDurationS duration can be remembered.
   */
  def calculateNumBatchesToRemember(batchDuration: Duration,
                                    minRememberDurationS: Duration): Int = {
    math.ceil(minRememberDurationS.milliseconds.toDouble / batchDuration.milliseconds).toInt
  }
}
