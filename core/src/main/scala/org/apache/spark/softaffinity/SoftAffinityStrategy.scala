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

package org.apache.spark.softaffinity

import scala.collection.mutable.LinkedHashSet
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

trait SoftAffinityAllocationTrait {

  def softAffinityReplicationNum(): Int = SparkEnv.get.conf.getInt(
      SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_REPLICATION_NUM,
      SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_REPLICATION_NUM_DEFAULT_VALUE
    )

  /**
   * allocate target executors for file
   */
  def allocateExecs(file: String,
                    candidates: ListBuffer[Option[(String, String)]]): Array[(String, String)]
}


class SoftAffinityStrategy extends SoftAffinityAllocationTrait with Logging {
  /**
   * allocate target executors for file
   */
  override def allocateExecs(file: String,
      candidates: ListBuffer[Option[(String, String)]]): Array[(String, String)] = {
    if (candidates.size < 1) {
      Array.empty
    } else {
      val candidatesSize = candidates.size
      val resultSet = new LinkedHashSet[(String, String)]

      val mod = file.hashCode % candidatesSize
      val c1 = if (mod < 0) (mod + candidatesSize) else mod
      // check whether the executor with index c1 is down
      if (candidates(c1).isDefined) {
        resultSet.add(candidates(c1).get)
      }
      val softAffinityReplication = softAffinityReplicationNum
      for (i <- 1 to (softAffinityReplication - 1)) {
        val c2 = (c1 + i) % candidatesSize
        if (candidates(c2).isDefined) {
          resultSet.add(candidates(c2).get)
        }
      }
      resultSet.toArray
    }
  }
}
