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

import java.io.File

import org.apache.spark.util.cleanup.{CleanupTask, CleanExternalList}
import org.apache.spark.util.collection.ExternalList

/**
 * Asynchronous cleaner for objects created on the Executor. So far
 * only supports cleaning up ExternalList objects. Equivalent to ContextCleaner
 * but for objects on the Executor heap.
 */
private[spark] class ExecutorCleaner extends WeakReferenceCleaner {

  def registerExternalListForCleanup(list: ExternalList[_]): Unit = {
    registerForCleanup(list, CleanExternalList(list.getBackingFileLocations()))
  }

  def doCleanExternalList(paths: Iterable[String]): Unit = {
    paths.map(path => new File(path)).foreach(f => {
      if (f.exists()) {
        val isDeleted = f.delete()
        if (!isDeleted) {
          logWarning(s"Failed to delete ${f.getAbsolutePath} backing ExternalList")
        }
      }
    })
  }

  override protected def handleCleanupForSpecificTask(task: CleanupTask): Unit = {
    task match {
      case CleanExternalList(paths) => doCleanExternalList(paths)
      case unknown => logWarning(s"Got cleanup task that cannot be" +
        s" handled by ExecutorCleaner: $unknown")
    }
  }

  override protected def cleanupThreadName(): String = "Executor Cleaner"
}
