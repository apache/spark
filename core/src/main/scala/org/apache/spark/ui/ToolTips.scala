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

package org.apache.spark.ui

private[spark] object ToolTips {
  val SCHEDULER_DELAY =
    """Scheduler delay includes time to ship the task from the scheduler to
       the executor, and time to send the task result from the executor to the scheduler. If
       scheduler delay is large, consider decreasing the size of tasks or decreasing the size
       of task results."""

  val INPUT = "Bytes read from Hadoop or from Spark storage."

  val SHUFFLE_WRITE = "Bytes written to disk in order to be read by a shuffle in a future stage."

  val SHUFFLE_READ =
    """Bytes read from remote executors. Typically less than shuffle write bytes
       because this does not include shuffle data read locally."""
}
