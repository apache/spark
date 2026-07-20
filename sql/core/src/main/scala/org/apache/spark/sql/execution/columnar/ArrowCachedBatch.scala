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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatch

/**
 * A [[SimpleMetricsCachedBatch]] implementation that stores one Arrow RecordBatch as an internal,
 * schema-less encapsulated IPC RecordBatch message.
 *
 * The payload is deliberately NOT a complete Arrow IPC stream: it carries no Schema message and
 * no end-of-stream marker, so a standard `ArrowStreamReader` cannot consume it. The schema is
 * constant for the whole cached relation and is reconstructed from the relation's attributes on
 * read (via `deserializeRecordBatch`), which avoids repeating the schema bytes in every cached
 * batch. This is an internal cache format, not an interchange format.
 *
 * The batch contains:
 *  - `numRows`: Number of rows in this batch
 *  - `arrowData`: One encapsulated Arrow RecordBatch message (with optional compression)
 *  - `stats`: Per-column statistics for partition pruning (upperBound, lowerBound, nullCount, etc.)
 *
 * This format enables:
 *  - Zero-copy columnar reads when output is ColumnarBatch with ArrowColumnVector
 *  - Off-heap memory management via Arrow allocators during encode/decode
 *  - Built-in compression support (zstd, lz4) at Arrow level
 *
 * @param numRows Number of rows in this cached batch
 * @param arrowData One encapsulated Arrow RecordBatch message
 * @param stats Per-column statistics as InternalRow (5 fields per column:
 *              upperBound, lowerBound, nullCount, rowCount, sizeInBytes)
 */
case class ArrowCachedBatch(
    numRows: Int,
    arrowData: Array[Byte],
    stats: InternalRow) extends SimpleMetricsCachedBatch
