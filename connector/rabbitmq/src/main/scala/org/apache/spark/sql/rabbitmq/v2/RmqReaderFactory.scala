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
package org.apache.spark.sql.rabbitmq.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

object RmqReaderFactory extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RmqInputPartition]
    new PartitionReader[InternalRow] with Logging {
      private var idx = -1
      private val size = p.rows.size()
      private var current: InternalRow = _

      logDebug(s"[RMQ V2] reader: start with $size rows")

      override def next(): Boolean = {
        idx += 1
        if (idx < size) {
          current = p.rows.get(idx)
          true
        } else {
          false
        }
      }

      override def get(): InternalRow = current

      override def close(): Unit = ()
    }
  }
}
