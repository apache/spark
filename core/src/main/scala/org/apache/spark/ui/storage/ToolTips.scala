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

package org.apache.spark.ui.storage

private[ui] object ToolTips {

  val RDD_NAME =
    "Name of the persisted RDD"

  val STORAGE_LEVEL =
    "StorageLevel displays where the persisted RDD is stored, " +
      "format of the persisted RDD (serialized or de-serialized) and" +
      "replication factor of the persisted RDD"

  val CACHED_PARTITIONS =
    "Number of partitions cached"

  val FRACTION_CACHED =
    "Fraction of total partitions cached"

  val SIZE_IN_MEMORY =
    "Total size of partitions in memory"

  val SIZE_ON_DISK =
    "Total size of partitions on the disk"
}

