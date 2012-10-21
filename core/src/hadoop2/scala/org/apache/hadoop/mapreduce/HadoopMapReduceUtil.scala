package org.apache.hadoop.mapreduce

import org.apache.hadoop.conf.Configuration
import task.{TaskAttemptContextImpl, JobContextImpl}

trait HadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = new JobContextImpl(conf, jobId)

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
}
