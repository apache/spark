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

package org.apache.spark.sql.catalyst.util

/**
 * A use of a table column that needs its values to be comparable, which a FILE is not. Each one
 * is a subclass of the `FILE_TYPE_UNSUPPORTED_COLUMN_USAGE` error condition.
 */
sealed abstract class FileTypeColumnUsage(val errorSubClass: String)

object FileTypeColumnUsage {
  case object PartitionColumn extends FileTypeColumnUsage("PARTITION_COLUMN")
  case object BucketColumn extends FileTypeColumnUsage("BUCKET_COLUMN")
  case object BucketSortColumn extends FileTypeColumnUsage("BUCKET_SORT_COLUMN")
  case object ClusteringColumn extends FileTypeColumnUsage("CLUSTERING_COLUMN")
  case object WriteOrderingColumn extends FileTypeColumnUsage("WRITE_ORDERING_COLUMN")
}
