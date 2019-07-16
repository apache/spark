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

import java.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.v2.{Table, TableCapability}
import org.apache.spark.sql.types.StructType

/**
 * An interface for systems that can collect the results of a streaming query. In order to preserve
 * exactly once semantics a sink must be idempotent in the face of multiple attempts to add the same
 * batch.
 *
 * Note that, we extends `Table` here, to make the v1 streaming sink API be compatible with
 * data source v2.
 */
trait Sink extends Table {

  /**
   * Adds a batch of data to this sink. The data for a given `batchId` is deterministic and if
   * this method is called more than once with the same batchId (which will happen in the case of
   * failures), then `data` should only be added once.
   *
   * Note 1: You cannot apply any operators on `data` except consuming it (e.g., `collect/foreach`).
   * Otherwise, you may get a wrong result.
   *
   * Note 2: The method is supposed to be executed synchronously, i.e. the method should only return
   * after data is consumed by sink successfully.
   */
  def addBatch(batchId: Long, data: DataFrame): Unit

  override def name: String = {
    throw new IllegalStateException("should not be called.")
  }

  override def schema: StructType = {
    throw new IllegalStateException("should not be called.")
  }

  override def capabilities: util.Set[TableCapability] = {
    throw new IllegalStateException("should not be called.")
  }
}
