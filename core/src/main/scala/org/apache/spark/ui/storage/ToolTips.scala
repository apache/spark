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
  val ID =
    "ID of RDD"

  val RDD_NAME =
    "Name of persisted RDD"

  val STORAGE_LEVEL =
    "StorageLevel displays where RDD is stored, " +
      "Format of RDD (Serialized or De-serialized) and" +
      "Replication factor of the RDD"

  val CACHED_PARTITIONS =
    "Number of partitions cached"

  val FRACTION_CACHED =
    "Fraction of RDD cached"

  val SIZE_IN_MEMORY =
    "Total size in the memory"

  val SIZE_ON_DISK =
    "Total size on the disk"
}

