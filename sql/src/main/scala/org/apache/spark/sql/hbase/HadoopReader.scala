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

package org.apache.spark.sql.hbase

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.hbase.util.InsertWappers._

/**
 * Helper class for scanning files stored in Hadoop - e.g., to read text file when bulk loading.
 */
private[hbase] class HadoopReader(
                                   @transient sc: SparkContext,
                                   path: String,
                                   delimiter: Option[String])(baseRelation: HBaseRelation) {
  /** make RDD[(SparkImmutableBytesWritable, SparkKeyValue)] from text file. */
  private[hbase] def makeBulkLoadRDDFromTextFile = {
    val rdd = sc.textFile(path)
    val splitRegex = delimiter.getOrElse(",")
    val relation = baseRelation
    // todo(wf): we should not reuse the buffer in bulk-loading otherwise it will lead to
    // corrupted as we are reusing same buffer
    rdd.mapPartitions { iter =>
      val keyBytes = new Array[(Array[Byte], DataType)](relation.keyColumns.size)
      val valueBytes =
        new Array[(Array[Byte], Array[Byte],Array[Byte])](relation.nonKeyColumns.size)
      val lineBuffer = HBaseKVHelper.createLineBuffer(relation.output)
      iter.map { line =>
        HBaseKVHelper.string2KV(line.split(splitRegex), relation,
          lineBuffer, keyBytes, valueBytes)
        val rowKeyData = HBaseKVHelper.encodingRawKeyColumns(keyBytes)
        val rowKey = new ImmutableBytesWritableWrapper(rowKeyData)
        val put = new PutWrapper(rowKeyData)
        valueBytes.foreach { case (family, qualifier, value) =>
          put.add(family, qualifier, value.clone())
        }
        (rowKey, put)
      }
    }
  }
}
