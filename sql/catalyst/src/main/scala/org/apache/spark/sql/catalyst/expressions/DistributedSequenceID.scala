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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern.{DISTRIBUTED_SEQUENCE_ID, TreePattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.storage.StorageLevelMapper

/**
 * Returns increasing 64-bit integers consecutive from 0.
 * The generated ID is guaranteed to be increasing consecutive started from 0.
 *
 * @note this expression is dedicated for Pandas API on Spark to use.
 */
case class DistributedSequenceID(storageLevel: Expression)
  extends LeafExpression with Unevaluable with NonSQLExpression {

  // This constructor is dedicated for Pandas API on Spark.
  // Get the storageLevel according to pandas_on_Spark.compute.default_index_cache.
  def this() = this(
    // Before `compute.default_index_cache` is explicitly set via
    // `ps.set_option`, `SQLConf.get` can not get its value (as well as its default value);
    // after `ps.set_option`, `SQLConf.get` can get its value:
    //
    //    In [1]: import pyspark.pandas as ps
    //    In [2]: ps.get_option("compute.default_index_cache")
    //    Out[2]: 'MEMORY_AND_DISK_SER'
    //    In [3]: spark.conf.get("pandas_on_Spark.compute.default_index_cache")
    //    ...
    //    Py4JJavaError: An error occurred while calling o40.get.
    //      : java.util.NoSuchElementException: pandas_on_Spark.compute.distributed_sequence_...
    //    at org.apache.spark.sql.errors.QueryExecutionErrors$.noSuchElementExceptionError...
    //    at org.apache.spark.sql.internal.SQLConf.$anonfun$getConfString$3(SQLConf.scala:4766)
    //    ...
    //    In [4]: ps.set_option("compute.default_index_cache", "NONE")
    //    In [5]: spark.conf.get("pandas_on_Spark.compute.default_index_cache")
    //    Out[5]: '"NONE"'
    //    In [6]: ps.set_option("compute.default_index_cache", "DISK_ONLY")
    //    In [7]: spark.conf.get("pandas_on_Spark.compute.default_index_cache")
    //    Out[7]: '"DISK_ONLY"'
    // The string is double quoted because of JSON ser/deser for pandas API on Spark
    Literal(
      SQLConf.get.getConfString("pandas_on_Spark.compute.default_index_cache",
        StorageLevelMapper.MEMORY_AND_DISK_SER.name()
      ).stripPrefix("\"").stripSuffix("\"")
    )
  )

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    DistributedSequenceID(storageLevel)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  final override val nodePatterns: Seq[TreePattern] = Seq(DISTRIBUTED_SEQUENCE_ID)

  override def nodeName: String = "distributed_sequence_id"
}
