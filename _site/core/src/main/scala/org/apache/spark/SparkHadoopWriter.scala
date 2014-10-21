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

package org.apache.spark

import java.io.IOException
import java.text.NumberFormat
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.mapred._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.HadoopRDD

/**
 * Internal helper class that saves an RDD using a Hadoop OutputFormat.
 *
 * Saves the RDD using a JobConf, which should contain an output key class, an output value class,
 * a filename to write to, etc, exactly like in a Hadoop MapReduce job.
 */
private[spark]
class SparkHadoopWriter(@transient jobConf: JobConf)
  extends Logging
  with SparkHadoopMapRedUtil
  with Serializable {

  private val now = new Date()
  private val conf = new SerializableWritable(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef,AnyRef] = null
  @transient private var format: OutputFormat[AnyRef,AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup() {
    setIDs(0, 0, 0)
    HadoopRDD.addLocalConfiguration("", 0, 0, 0, conf.value)

    val jCtxt = getJobContext()
    getOutputCommitter().setupJob(jCtxt)
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(now),
      jobid, splitID, attemptID, conf.value)
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: AnyRef, value: AnyRef) {
    if (writer != null) {
      writer.write(key, value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit() {
    val taCtxt = getTaskContext()
    val cmtr = getOutputCommitter()
    if (cmtr.needsTaskCommit(taCtxt)) {
      try {
        cmtr.commitTask(taCtxt)
        logInfo (taID + ": Committed")
      } catch {
        case e: IOException => {
          logError("Error committing the output of task: " + taID.value, e)
          cmtr.abortTask(taCtxt)
          throw e
        }
      }
    } else {
      logInfo ("No need to commit output of task: " + taID.value)
    }
  }

  def commitJob() {
    // always ? Or if cmtr.needsTaskCommit ?
    val cmtr = getOutputCommitter()
    cmtr.commitJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef,AnyRef] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[OutputFormat[AnyRef,AnyRef]]
    }
    format
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) {
      committer = conf.value.getOutputCommitter
    }
    committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) {
      jobContext = newJobContext(conf.value, jID.value)
    }
    jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) {
      taskContext =  newTaskAttemptContext(conf.value, taID.value)
    }
    taskContext
  }

  private def setIDs(jobid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID](
        new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }
}

private[spark]
object SparkHadoopWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(time)
    new JobID(jobtrackerID, id)
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs)
  }
}
