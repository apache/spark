
package org.apache.hadoop.mapred

import org.apache.hadoop.mapreduce.TaskType

trait HadoopMapRedUtil {
  def newJobContext(conf: JobConf, jobId: JobID): JobContext = new JobContextImpl(conf, jobId)

  def newTaskAttemptContext(conf: JobConf, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

  def newTaskAttemptID(jtIdentifier: String, jobId: Int, isMap: Boolean, taskId: Int, attemptId: Int) =
    new TaskAttemptID(jtIdentifier, jobId, if (isMap) TaskType.MAP else TaskType.REDUCE, taskId, attemptId)
}
