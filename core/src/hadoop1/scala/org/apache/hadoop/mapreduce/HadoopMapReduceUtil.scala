package org.apache.hadoop.mapreduce

import org.apache.hadoop.conf.Configuration

trait HadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = new JobContext(conf, jobId)

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContext(conf, attemptId)
}
