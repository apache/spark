package org.apache.hadoop.mapreduce

import org.apache.hadoop.conf.Configuration

trait HadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = new JobContext(conf, jobId)

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContext(conf, attemptId)

  def newTaskAttemptID(jtIdentifier: String, jobId: Int, isMap: Boolean, taskId: Int, attemptId: Int) = new TaskAttemptID(jtIdentifier,
    jobId, isMap, taskId, attemptId)
}
