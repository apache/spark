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

package org.apache.spark

/**
 * Util methods which only used in task function.
 */
object TaskUtils {
  /**
   * This sleep method will check whether task is killed. If yes, throw TaskKilledException
   */
  def sleep(durationMillis: Long): Unit = {

    val taskContext = TaskContext.get()
    val begTime = System.currentTimeMillis()

    val checkInterval = 100L // 100ms
    var remainingTime = durationMillis

    while (remainingTime > checkInterval) {
      Thread.sleep(checkInterval)
      remainingTime = durationMillis - (System.currentTimeMillis() - begTime)
      taskContext.killTaskIfInterrupted()
    }
    Thread.sleep(remainingTime)
  }
}
