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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.ApplicationId

/**
 * Simple Testing Application Id; ID and cluster timestamp are set in constructor
 * and cannot be updated.
 * @param id app id
 * @param clusterTimestamp timestamp
 */
private[spark] class StubApplicationId(id: Int, clusterTimestamp: Long) extends ApplicationId {
  override def getId: Int = {
    id
  }

  override def getClusterTimestamp: Long = {
    clusterTimestamp
  }

  override def setId(id: Int): Unit = {}

  override def setClusterTimestamp(clusterTimestamp: Long): Unit = {}

  override def build(): Unit = {}
}
