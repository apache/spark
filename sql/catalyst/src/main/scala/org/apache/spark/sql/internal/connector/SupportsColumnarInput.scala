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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An internal mixin for a `Write`, or for the `StreamingWrite` of a micro-batch write, whose data
 * writers are [[ColumnarInputDataWriter]]s. When the query of an append or an overwrite, or of a
 * micro-batch, produces columnar batches, Spark passes them to the data writers instead of
 * converting them to rows first.
 */
// TODO: design an official API for columnar writes, as for columnar reads.
trait SupportsColumnarInput

/**
 * A data writer that also takes the data to write as columnar batches, see
 * [[SupportsColumnarInput]]. A task writes either rows or batches. Like a row, a batch is only
 * valid during the call: its producer may reuse it for the next batch.
 */
trait ColumnarInputDataWriter extends DataWriter[InternalRow] {
  def writeBatch(batch: ColumnarBatch): Unit
}
