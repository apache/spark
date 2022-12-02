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

package org.apache.spark.sql.execution.python

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * A physical plan that adds a new long column with `sequenceAttr` that
 * increases one by one. This is for 'distributed-sequence' default index
 * in pandas API on Spark.
 */
case class AttachDistributedSequenceExec(
    sequenceAttr: Attribute,
    child: SparkPlan)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(sequenceAttr)

  override val output: Seq[Attribute] = sequenceAttr +: child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  @transient private var cached: RDD[InternalRow] = _

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    // before `compute.default_index_cache` is explicitly set via
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
    val storageLevel = SQLConf.get.getConfString(
      "pandas_on_Spark.compute.default_index_cache",
      "MEMORY_AND_DISK_SER"
    ).stripPrefix("\"").stripSuffix("\"")

    val cachedRDD = storageLevel match {
      // zipWithIndex launches a Spark job only if #partition > 1
      case _ if childRDD.getNumPartitions <= 1 => childRDD

      case "NONE" => childRDD

      case "LOCAL_CHECKPOINT" =>
        // localcheckpointing is unreliable so should not eagerly release it in 'cleanupResources'
        childRDD.map(_.copy()).localCheckpoint()
          .setName(s"Temporary RDD locally checkpointed in AttachDistributedSequenceExec($id)")

      case _ =>
        cached = childRDD.map(_.copy()).persist(StorageLevel.fromString(storageLevel))
          .setName(s"Temporary RDD cached in AttachDistributedSequenceExec($id)")
        cached
    }

    cachedRDD.zipWithIndex().mapPartitions { iter =>
      val unsafeProj = UnsafeProjection.create(output, output)
      val joinedRow = new JoinedRow
      val unsafeRowWriter =
        new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1)

      iter.map { case (row, id) =>
        // Writes to an UnsafeRow directly
        unsafeRowWriter.reset()
        unsafeRowWriter.write(0, id)
        joinedRow(unsafeRowWriter.getRow, row)
      }.map(unsafeProj)
    }
  }

  override protected[sql] def cleanupResources(): Unit = {
    try {
      if (cached != null && cached.getStorageLevel != StorageLevel.NONE) {
        logWarning(s"clean up cached RDD(${cached.id}) in AttachDistributedSequenceExec($id)")
        cached.unpersist(blocking = false)
      }
    } finally {
      super.cleanupResources()
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AttachDistributedSequenceExec =
    copy(child = newChild)

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val indexColumn = s"Index: $sequenceAttr"
    s"$nodeName$truncatedOutputString $indexColumn"
  }
}
