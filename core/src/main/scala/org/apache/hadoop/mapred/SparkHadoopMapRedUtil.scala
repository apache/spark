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

package org.apache.hadoop.mapred

private[apache]
trait SparkHadoopMapRedUtil {
  def newJobContext(conf: JobConf, jobId: JobID): JobContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.JobContextImpl",
      "org.apache.hadoop.mapred.JobContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf],
      classOf[org.apache.hadoop.mapreduce.JobID])
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: JobConf, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.TaskAttemptContextImpl",
      "org.apache.hadoop.mapred.TaskAttemptContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  def newTaskAttemptID(
      jtIdentifier: String,
      jobId: Int,
      isMap: Boolean,
      taskId: Int,
      attemptId: Int) = {
    new TaskAttemptID(jtIdentifier, jobId, isMap, taskId, attemptId)
  }

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      Class.forName(first)
    } catch {
      case e: ClassNotFoundException =>
        Class.forName(second)
    }
  }
}
