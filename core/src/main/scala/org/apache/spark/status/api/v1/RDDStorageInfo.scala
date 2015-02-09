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
package org.apache.spark.status.api.v1

//Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage page ... does
// anybody pay attention to it?
case class RDDStorageInfo(
  id: Int,
  name: String,
  numPartitions: Int,
  numCachedPartitions: Int,
  storageLevel: String,
  memoryUsed: Long,
  diskUsed: Long,
  dataDistribution: Option[Seq[RDDDataDistribution]],
  partitions: Option[Seq[RDDPartitionInfo]]
)

case class RDDDataDistribution(
  address: String,
  memoryUsed: Long,
  memoryRemaining: Long,
  diskUsed: Long
)

case class RDDPartitionInfo(
  blockName: String,
  storageLevel: String,
  memoryUsed: Long,
  diskUsed: Long,
  executors: Seq[String]
)