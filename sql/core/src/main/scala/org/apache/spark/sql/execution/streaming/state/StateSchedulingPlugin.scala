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

package org.apache.spark.sql.execution.streaming.state

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{Task, TaskLocation, TaskSchedulingPlugin}

class StateSchedulingPlugin(
    val rdd: RDD[_],
    val storeCoordinator: Option[StateStoreCoordinatorRef],
    val getStateProviderId: (Int => StateStoreProviderId))
  extends TaskSchedulingPlugin with Logging {

  /**
   * Return the mapping between state store location to task indexes/partitions,
   * e.g. "host1_exec1" -> Seq(0, 1) => task 0, 1 have been running state stores on exec1 of host1.
   */
  private def stateLocations = {
    (0 until rdd.getNumPartitions).flatMap { index =>
      val stateStoreProviderId = getStateProviderId(index)

      val optLoc = storeCoordinator.flatMap(_.getLocation(stateStoreProviderId))
      optLoc.map(loc => loc -> index)
    }.groupBy(_._1).mapValues(values => values.map(v => v._2))
  }

  val taskNumPerExec = mutable.HashMap.empty[String, Int]

  // The maximum allowed assigned tasks per executor.
  private val maxTasksPerExec: Int = {
    val executors = rdd.sparkContext.getExecutorIds()

    if (executors.isEmpty) {
      // The current scheduler does not support getting executors.
      // TODO: Maybe make it configurable?
      1
    } else {
      rdd.getNumPartitions / rdd.sparkContext.getExecutorIds().length
    }
  }

  override def rankTasks(
      execId: String,
      host: String,
      tasks: Seq[Task[_]],
      taskIndexes: Seq[Int]): Seq[Int] = {
    val offerLoc = TaskLocation(host, execId).toString
    val returned = if (taskIndexes.isEmpty) {
      taskIndexes
    } else {

      if (stateLocations.contains(offerLoc)) {
        // If we have state store on the host/exec.
        val currentLocs = stateLocations
        val candidates = currentLocs(offerLoc)
        taskIndexes.zipWithIndex.find(pair => candidates.contains(pair._1)).map { pref =>
          // Pick up one task that was running state store on the host/exec.
          Seq(pref._2)
        }.getOrElse {
          if (candidates.length >= maxTasksPerExec) {
            // Reaches maximum of assigned tasks on the executor, do not assign new task.
            Seq.empty
          } else {
            // Finds a task which was not assigned to other executors previously.
            val assignedTasks = currentLocs.values.flatten.toSeq
            val unassigned = taskIndexes.zipWithIndex
              .filter(p => !assignedTasks.contains(p._1))
              .map(_._2)
            if (unassigned.nonEmpty) {
              Seq(unassigned.head)
            } else {
              Seq.empty
            }
          }
        }
      } else {
        // The host/exec has no state store running. Might be the first micro-batch.
        // Finds a task which was not assigned to other executors previously.
        // TODO: if there are executors lost, and new executors are allocated, we should
        // be able to assign new tasks to new executors.
        val currentLocs = stateLocations
        val assignedTasks = currentLocs.values.flatten.toSeq
        val unassigned = taskIndexes.zipWithIndex
          .filter(p => !assignedTasks.contains(p._1))
          .map(_._2)
        val currentNum = taskNumPerExec.getOrElse(offerLoc, 0)
        if (unassigned.nonEmpty && currentNum < maxTasksPerExec) {
          taskNumPerExec(offerLoc) = currentNum + 1
          Seq(unassigned.head)
        } else {
          Seq.empty
        }
      }
    }
    logInfo(s"assigned: $returned for offerLoc: $offerLoc.")
    returned
  }
}
