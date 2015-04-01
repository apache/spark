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

package org.apache.spark.scheduler.cluster.mesos

import scala.collection.mutable

import org.apache.spark.deploy.mesos.MesosDriverDescription

/**
 * A request queue for launching drivers in Mesos cluster mode.
 * This queue automatically stores the state after each pop/push
 * so it can be recovered later.
 * This queue is also bounded and rejects offers when it's full.
 * @param state Mesos state abstraction to fetch persistent state.
 */
private[mesos] class DriverQueue(state: MesosClusterPersistenceEngine, capacity: Int) {
  var queue: mutable.Queue[MesosDriverDescription] = new mutable.Queue[MesosDriverDescription]()
  private var count = 0

  initialize()

  def initialize(): Unit = {
    state.fetchAll[MesosDriverDescription]().foreach(d => queue.enqueue(d))

    // This size might be larger than the passed in capacity, but we allow
    // this so we don't lose queued drivers.
    count = queue.size
  }

  def isFull = count >= capacity

  def size: Int = count

  def contains(submissionId: String): Boolean = {
    queue.exists(s => s.submissionId.equals(submissionId))
  }

  def offer(submission: MesosDriverDescription): Boolean = {
    if (isFull) {
      return false
    }

    queue.enqueue(submission)
    state.persist(submission.submissionId.get, submission)
    true
  }

  def remove(submissionId: String): Boolean = {
    val removed = queue.dequeueFirst(d => d.submissionId.equals(submissionId))
    if (removed.isDefined) {
      state.expunge(removed.get.submissionId.get)
    }

    removed.isDefined
  }

  def peek(): Option[MesosDriverDescription] = {
    queue.headOption
  }

  def poll(): Option[MesosDriverDescription] = {
    if (queue.isEmpty) {
      None
    } else {
      val item = queue.dequeue()
      state.expunge(item.submissionId.get)
      Some(item)
    }
  }

  // Returns a copy of the queued drivers.
  def drivers: Iterable[MesosDriverDescription] = {
    val buffer = new Array[MesosDriverDescription](queue.size)
    queue.copyToArray(buffer)
    buffer
  }

}
