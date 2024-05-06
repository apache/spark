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

package org.apache.spark.sql.connect.client

/**
 * Scheduler the remote server is running to schedule tasks.
 *
 * Note: The spark connect client cannot change the scheduling mode.
 *
 * @param mode
 *   FIFO or FAIR.
 * @param pools
 *   Tasks will be enqueued into these pools.
 */
class SchedulingMode(val mode: SchedulingMode.Mode, val pools: Seq[SchedulerPool]) {
  /**
   * Return the pool associated with the given name, if noe exists.
   */
  def getPoolForName(pool: String): Option[SchedulerPool] = {
    pools.find(_.name == pool)
  }
}

/**
 * Spark support FIFO and FAIR scheduler, the former will queue up tasks behind each other, the
 * latter will share the pool's resources fairly. The FAIR scheduler also supports grouping jobs
 * into pools and setting different scheduling options (e.g. weight) for each pool.
 */
object SchedulingMode extends Enumeration {
  type Mode = Value
  val FAIR, FIFO = Value
}

/**
 * Schedulable pool for tasks.
 *
 * Note: The spark connect client cannot create a new pool, but can choose which pool to run jobs.
 *
 * @param name
 *   Pool name.
 * @param mode
 *   FIFO or FAIR.
 * @param weight
 *   Pool's share of the cluster relative to other pools.
 * @param minShare
 *   A pool can always get up to minShare number of resources.
 */
case class SchedulerPool(name: String, mode: SchedulingMode.Mode, weight: Int, minShare: Int)
