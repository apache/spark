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

import java.io.{ObjectInputStream, IOException}
import scala.collection.mutable
import scala.collection.mutable.{HashSet, HashMap}
import scala.reflect.ClassTag
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.TimeStampedHashMap


private[streaming]
class FileInputDStream[K: ClassTag, V: ClassTag, F <: NewInputFormat[K,V] : ClassTag](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true)
  extends InputDStream[(K, V)](ssc_) {

  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  // all files found
  private val foundFiles = new HashMap[String, Long]
  if (newFilesOnly) {
    foundFiles ++= fs.listStatus(directoryPath).map(f => (f.getPath.toString, f.getLen))
  }

  // Files with mod time earlier than this is ignored. This is updated every interval
  // such that in the current interval, files older than any file found in the
  // previous interval will be ignored. Obviously this time keeps moving forward.
  private var ignoreTime = if (newFilesOnly) System.currentTimeMillis() else 0L

  // Latest file mod time seen till any point of time
  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null
  @transient private[streaming] var files = new HashMap[Time, Array[(String, Long, Long)]]

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
    assert(validTime.milliseconds >= ignoreTime,
      "Trying to get new files for a really old time [" + validTime + " < " + ignoreTime + "]")

    // Find new files
    val (newFiles, minNewFileModTime) = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    if (!newFiles.isEmpty) {
      foundFiles ++= newFiles.map(x => (x._1, x._3))
      ignoreTime = minNewFileModTime
    }
    files += ((validTime, newFiles.toArray))
    Some(filesToRDD(newFiles))
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldFiles = files.filter(_._1 < (time - rememberDuration))
    files --= oldFiles.keys
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
  }

  /**
   * Find files which have modification timestamp <= current time and return a 3-tuple of
   * (new files found, latest modification time among them, files with latest modification time)
   */
  private def findNewFiles(currentTime: Long): (Seq[(String, Long, Long)], Long) = {
    logDebug("Trying to get new files for time " + currentTime)
    val lastNewFileFindingTime = System.currentTimeMillis
    val filter = new CustomPathFilter(currentTime)
    val newFiles = fs.listStatus(directoryPath).map(filter(_)).filter(_._1 != null)
    // clean up not-existed files
    foundFiles --= foundFiles.keys.filterNot(filter.allFiles.contains)
    val timeTaken = System.currentTimeMillis - lastNewFileFindingTime
    logInfo("Finding new files took " + timeTaken + " ms")
    if (timeTaken > slideDuration.milliseconds) {
      logWarning(
        "Time taken to find new files exceeds the batch size. " +
          "Consider increasing the batch size or reducing the number of " +
          "files in the monitored directory."
      )
    }
    (newFiles, filter.minNewFileModTime)
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[(String, Long, Long)]): RDD[(K, V)] = {
    val fileRDDs = files.map{case (path: String, start: Long, end: Long) =>
      context.sparkContext.partialHadoopFile[K, V, F](path, start, end - start)}
    files.zip(fileRDDs).foreach { case (file, rdd) => {
      if (rdd.partitions.size == 0) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
    }}
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(new Configuration())
    fs_
  }

  private def reset()  {
    fs_ = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new HashMap[Time, RDD[(K,V)]] ()
    files = new HashMap[Time, Array[(String, Long, Long)]]
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    def hadoopFiles = data.asInstanceOf[HashMap[Time, Array[(String, Long, Long)]]]

    override def update(time: Time) {
      hadoopFiles.clear()
      hadoopFiles ++= files  // copy
    }

    override def cleanup(time: Time) { }

    override def restore() {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          files += ((t, f))
          generatedRDDs += ((t, filesToRDD(f)))
        }
      }
    }

    override def toString() = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }

  /**
   * Custom PathFilter class to find new files that
   * ... have modification time more than ignore time
   * ... have not been seen in the last interval
   * ... have modification time less than maxModTime
   * ... have size larger than in foundFiles
   *
   * Return (path, start, end)
   */
  private[streaming]
  class CustomPathFilter(maxModTime: Long) {

    // Minimum of the mod times of new files found in the current interval
    var minNewFileModTime = -1L
    var allFiles = new mutable.HashSet[String]()

    def apply(status: FileStatus): (String, Long, Long) = {
      try {
        val path = status.getPath
        if (!filter(path)) {  // Reject file if it does not satisfy filter
          logDebug("Rejected by filter " + path)
          return (null, 0, 0)
        }
        allFiles.add(path.toString)
        val modTime = status.getModificationTime
        logDebug("Mod time for " + path + " is " + modTime)
        if (modTime < ignoreTime) {
          // Reject file if it was created before the ignore time (or, before last interval)
          logDebug("Mod time " + modTime + " less than ignore time " + ignoreTime)
          return (null, 0, 0)
        } else if (modTime > maxModTime) {
          // Reject file if it is too new that considering it may give errors
          logDebug("Mod time more than ")
          return (null, 0, 0)
        }
        val lastSize = foundFiles.getOrElse(path.toString, 0L)
        val size = status.getLen
        if (size <= lastSize) {
          logDebug("no new data")
          return (null, 0, 0)
        }
        if (minNewFileModTime < 0 || modTime < minNewFileModTime) {
          minNewFileModTime = modTime
        }
        logDebug(s"Accepted $path from $lastSize to $size")
        (path.toString, lastSize, size)
      } catch {
        case fnfe: java.io.FileNotFoundException =>
          logWarning("Error finding new files", fnfe)
          reset()
          return (null, 0, 0)
      }
    }
  }
}

private[streaming]
object FileInputDStream {
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")
}
