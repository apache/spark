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

package org.apache.spark.mapreduce

import java.lang.{Boolean => JBoolean, Integer => JInteger}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobContext, JobID, TaskAttemptContext, TaskAttemptID}

private[spark]
trait SparkHadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = {
    val klass = firstAvailableClass(
        "org.apache.hadoop.mapreduce.task.JobContextImpl",  // hadoop2, hadoop2-yarn
        "org.apache.hadoop.mapreduce.JobContext")           // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[JobID])
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass(
        "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl",  // hadoop2, hadoop2-yarn
        "org.apache.hadoop.mapreduce.TaskAttemptContext")           // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  def newTaskAttemptID(
      jtIdentifier: String,
      jobId: Int,
      isMap: Boolean,
      taskId: Int,
      attemptId: Int): TaskAttemptID = {
    val klass = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptID")
    try {
      // First, attempt to use the old-style constructor that takes a boolean isMap
      // (not available in YARN)
      val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], classOf[Boolean],
        classOf[Int], classOf[Int])
      ctor.newInstance(jtIdentifier, new JInteger(jobId), new JBoolean(isMap), new JInteger(taskId),
        new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
    } catch {
      case exc: NoSuchMethodException => {
        // If that failed, look for the new constructor that takes a TaskType (not available in 1.x)
        val taskTypeClass = Class.forName("org.apache.hadoop.mapreduce.TaskType")
          .asInstanceOf[Class[Enum[_]]]
        val taskType = taskTypeClass.getMethod("valueOf", classOf[String]).invoke(
          taskTypeClass, if(isMap) "MAP" else "REDUCE")
        val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], taskTypeClass,
          classOf[Int], classOf[Int])
        ctor.newInstance(jtIdentifier, new JInteger(jobId), taskType, new JInteger(taskId),
          new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
      }
    }
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
