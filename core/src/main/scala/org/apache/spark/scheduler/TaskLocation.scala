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

package org.apache.spark.scheduler

/**
 * A location where a task should run. This can either be a host or a (host, executorID) pair.
 * In the latter case, we will prefer to launch the task on that executorID, but our next level
 * of preference will be executors on the same host if this is not possible.
 */
private[spark]
class TaskLocation private (val host: String, val executorId: Option[String]) extends Serializable {
  override def toString: String = "TaskLocation(" + host + ", " + executorId + ")"
}

private[spark] object TaskLocation {
  def apply(host: String, executorId: String) = new TaskLocation(host, Some(executorId))

  def apply(host: String) = new TaskLocation(host, None)
}
