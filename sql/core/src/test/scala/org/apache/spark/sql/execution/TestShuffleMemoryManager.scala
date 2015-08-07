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

package org.apache.spark.sql.execution

import org.apache.spark.shuffle.ShuffleMemoryManager

/**
 * A [[ShuffleMemoryManager]] that can be controlled to run out of memory.
 */
class TestShuffleMemoryManager extends ShuffleMemoryManager(Long.MaxValue, 4 * 1024 * 1024) {
  private var oom = false

  override def tryToAcquire(numBytes: Long): Long = {
    if (oom) {
      oom = false
      0
    } else {
      // Uncomment the following to trace memory allocations.
      // println(s"tryToAcquire $numBytes in " +
      //   Thread.currentThread().getStackTrace.mkString("", "\n  -", ""))
      val acquired = super.tryToAcquire(numBytes)
      acquired
    }
  }

  override def release(numBytes: Long): Unit = {
    // Uncomment the following to trace memory releases.
    // println(s"release $numBytes in " +
    //   Thread.currentThread().getStackTrace.mkString("", "\n  -", ""))
    super.release(numBytes)
  }

  def markAsOutOfMemory(): Unit = {
    oom = true
  }
}
