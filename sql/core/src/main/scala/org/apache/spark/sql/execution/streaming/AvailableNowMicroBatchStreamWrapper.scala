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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream

/**
 * This class wraps a [[MicroBatchStream]] and makes it supports Trigger.AvailableNow.
 *
 * See [[AvailableNowDataStreamWrapper]] for more details.
 */
class AvailableNowMicroBatchStreamWrapper(delegate: MicroBatchStream)
  extends AvailableNowDataStreamWrapper(delegate) with MicroBatchStream {

  override def latestOffset(): streaming.Offset = throw new UnsupportedOperationException(
    "latestOffset(Offset, ReadLimit) should be called instead of this method")

  override def planInputPartitions(start: streaming.Offset, end: streaming.Offset):
  Array[InputPartition] = delegate.planInputPartitions(start, end)

  override def createReaderFactory(): PartitionReaderFactory = delegate.createReaderFactory()
}
