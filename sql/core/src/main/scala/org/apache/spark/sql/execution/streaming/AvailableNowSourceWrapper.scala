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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * This class wraps a [[Source]] and makes it supports Trigger.AvailableNow.
 *
 * See [[AvailableNowDataStreamWrapper]] for more details.
 */
class AvailableNowSourceWrapper(delegate: Source)
  extends AvailableNowDataStreamWrapper(delegate) with Source {

  override def schema: StructType = delegate.schema

  override def getOffset: Option[Offset] =
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3166")

  override def getBatch(start: Option[Offset], end: Offset): DataFrame =
    delegate.getBatch(start, end)
}
