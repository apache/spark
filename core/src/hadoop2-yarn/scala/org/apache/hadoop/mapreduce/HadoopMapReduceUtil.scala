package org.apache.hadoop.mapreduce

import org.apache.hadoop.conf.Configuration
import task.{TaskAttemptContextImpl, JobContextImpl}

trait HadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = new JobContextImpl(conf, jobId)

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

  def newTaskAttemptID(jtIdentifier: String, jobId: Int, isMap: Boolean, taskId: Int, attemptId: Int) =
    new TaskAttemptID(jtIdentifier, jobId, if (isMap) TaskType.MAP else TaskType.REDUCE, taskId, attemptId)
}
