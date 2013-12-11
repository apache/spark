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

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.streaming.{DStreamCheckpointData, StreamingContext, Time}

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import scala.collection.mutable.{HashSet, HashMap}
import java.io.{ObjectInputStream, IOException}

private[streaming]
class FileInputDStream[K: ClassManifest, V: ClassManifest, F <: NewInputFormat[K,V] : ClassManifest](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true) 
  extends InputDStream[(K, V)](ssc_) {

  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  // Latest file mod time seen till any point of time
  private val prevModTimeFiles = new HashSet[String]()
  private var prevModTime = 0L

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null
  @transient private[streaming] var files = new HashMap[Time, Array[String]]

  override def start() {
    if (newFilesOnly) {
      prevModTime = graph.zeroTime.milliseconds
    } else {
      prevModTime = 0
    }
    logDebug("LastModTime initialized to " + prevModTime + ", new files only = " + newFilesOnly)
  }
  
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
    assert(validTime.milliseconds >= prevModTime,
      "Trying to get new files for really old time [" + validTime + " < " + prevModTime)

    // Find new files
    val (newFiles, latestModTime, latestModTimeFiles) = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    if (newFiles.length > 0) {
      // Update the modification time and the files processed for that modification time
      if (prevModTime < latestModTime) {
        prevModTime = latestModTime
        prevModTimeFiles.clear()
      }
      prevModTimeFiles ++= latestModTimeFiles
      logDebug("Last mod time updated to " + prevModTime)
    }
    files += ((validTime, newFiles.toArray))
    Some(filesToRDD(newFiles))
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearOldMetadata(time: Time) {
    super.clearOldMetadata(time)
    val oldFiles = files.filter(_._1 <= (time - rememberDuration))
    files --= oldFiles.keys
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
  }

  /**
   * Finds files which have modification timestamp <= current time. If some files are being
   * deleted in the directory, then it can generate transient exceptions. Hence, multiple
   * attempts are made to handle these transient exceptions. Returns 3-tuple
   * (new files found, latest modification time among them, files with latest modification time)
   */
  private def findNewFiles(currentTime: Long): (Seq[String], Long, Seq[String]) = {
    logDebug("Trying to get new files for time " + currentTime)
    var attempts = 0
    while (attempts < FileInputDStream.MAX_ATTEMPTS) {
      attempts += 1
      try {
        val filter = new CustomPathFilter(currentTime)
        val newFiles = fs.listStatus(path, filter)
        return (newFiles.map(_.getPath.toString), filter.latestModTime, filter.latestModTimeFiles.toSeq)
      } catch {
        case ioe: IOException => logWarning("Attempt " + attempts + " to get new files failed", ioe)
      }
    }
    (Seq(), -1, Seq())
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    new UnionRDD(
      context.sparkContext,
      files.map(file => context.sparkContext.newAPIHadoopFile[K, V, F](file))
    )
  }

  private def path: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = path.getFileSystem(new Configuration())
    fs_
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new HashMap[Time, RDD[(K,V)]] ()
    files = new HashMap[Time, Array[String]]
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    def hadoopFiles = data.asInstanceOf[HashMap[Time, Array[String]]]

    override def update() {
      hadoopFiles.clear()
      hadoopFiles ++= files
    }

    override def cleanup() { }

    override def restore() {
      hadoopFiles.foreach {
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
   * PathFilter to find new files that have modification timestamps <= current time, but have not
   * been seen before (i.e. the file should not be in lastModTimeFiles)
   * @param currentTime
   */
  private[streaming]
  class CustomPathFilter(currentTime: Long) extends PathFilter() {
    // Latest file mod time seen in this round of fetching files and its corresponding files
    var latestModTime = 0L
    val latestModTimeFiles = new HashSet[String]()

    def accept(path: Path): Boolean = {
      if (!filter(path)) {  // Reject file if it does not satisfy filter
        logDebug("Rejected by filter " + path)
        return false
      } else {              // Accept file only if
      val modTime = fs.getFileStatus(path).getModificationTime()
        logDebug("Mod time for " + path + " is " + modTime)
        if (modTime < prevModTime) {
          logDebug("Mod time less than last mod time")
          return false  // If the file was created before the last time it was called
        } else if (modTime == prevModTime && prevModTimeFiles.contains(path.toString)) {
          logDebug("Mod time equal to last mod time, but file considered already")
          return false  // If the file was created exactly as lastModTime but not reported yet
        } else if (modTime > currentTime) {
          logDebug("Mod time more than valid time")
          return false  // If the file was created after the time this function call requires
        }
        if (modTime > latestModTime) {
          latestModTime = modTime
          latestModTimeFiles.clear()
          logDebug("Latest mod time updated to " + latestModTime)
        }
        latestModTimeFiles += path.toString
        logDebug("Accepted " + path)
        return true
      }
    }
  }
}

private[streaming]
object FileInputDStream {
  val MAX_ATTEMPTS = 10
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")
}


