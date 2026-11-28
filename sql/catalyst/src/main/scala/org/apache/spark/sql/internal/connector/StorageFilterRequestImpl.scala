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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.connector.read.{StorageFilter, StorageFilterRequest}

/**
 * The [[StorageFilterRequest]] the readers of a scan get. Spark builds one per scan and sends it
 * with each of its tasks. With a per-partition runtime bloom filter, a task builds one for each
 * partition key of its splits that has a bloom filter, see `PerPartitionStorageFilterRDD`.
 */
case class StorageFilterRequestImpl(filter: StorageFilter, appendsCheckedColumn: Boolean)
  extends StorageFilterRequest
