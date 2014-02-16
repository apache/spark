package org.apache.hadoop.mapred

import java.io.IOException
import java.text.NumberFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable

import org.apache.spark.Logging
import org.apache.spark.SerializableWritable

import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.FileSinkDesc

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on [[SparkHadoopWriter]].
 */
protected
class SharkHadoopWriter(
    @transient jobConf: JobConf,
    fileSinkConf: FileSinkDesc)
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

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient private var format: HiveOutputFormat[AnyRef, Writable] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup() {
    setIDs(0, 0, 0)
    setConfParams()

    val jCtxt = getJobContext()
    getOutputCommitter().setupJob(jCtxt)
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    setConfParams()
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)

    getOutputCommitter().setupTask(getTaskContext())
    writer = HiveFileFormatUtils.getHiveRecordWriter(
      conf.value,
      fileSinkConf.getTableInfo,
      conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
      fileSinkConf,
      new Path(path, outputName),
      null)
  }

  def write(value: Writable) {
    if (writer != null) {
      writer.write(value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    // Seems the boolean value passed into close does not matter.
    writer.close(false)
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
      logWarning ("No need to commit output of task: " + taID.value)
    }
  }

  def commitJob() {
    // always ? Or if cmtr.needsTaskCommit ?
    val cmtr = getOutputCommitter()
    cmtr.commitJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): HiveOutputFormat[AnyRef,Writable] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[HiveOutputFormat[AnyRef,Writable]]
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

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
  }
}

object SharkHadoopWriter {
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