// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Shared field inventory for the independent C and C++ layout checks. Keep it in header order.
// The including source defines SPARK_NATIVE_ABI_TYPE and SPARK_NATIVE_ABI_FIELD.

SPARK_NATIVE_ABI_TYPE(ArrowSchema)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, format)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, name)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, metadata)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, flags)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, n_children)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, children)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, dictionary)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, release)
SPARK_NATIVE_ABI_FIELD(ArrowSchema, private_data)

SPARK_NATIVE_ABI_TYPE(ArrowArray)
SPARK_NATIVE_ABI_FIELD(ArrowArray, length)
SPARK_NATIVE_ABI_FIELD(ArrowArray, null_count)
SPARK_NATIVE_ABI_FIELD(ArrowArray, offset)
SPARK_NATIVE_ABI_FIELD(ArrowArray, n_buffers)
SPARK_NATIVE_ABI_FIELD(ArrowArray, n_children)
SPARK_NATIVE_ABI_FIELD(ArrowArray, buffers)
SPARK_NATIVE_ABI_FIELD(ArrowArray, children)
SPARK_NATIVE_ABI_FIELD(ArrowArray, dictionary)
SPARK_NATIVE_ABI_FIELD(ArrowArray, release)
SPARK_NATIVE_ABI_FIELD(ArrowArray, private_data)

SPARK_NATIVE_ABI_TYPE(ArrowArrayStream)
SPARK_NATIVE_ABI_FIELD(ArrowArrayStream, get_schema)
SPARK_NATIVE_ABI_FIELD(ArrowArrayStream, get_next)
SPARK_NATIVE_ABI_FIELD(ArrowArrayStream, get_last_error)
SPARK_NATIVE_ABI_FIELD(ArrowArrayStream, release)
SPARK_NATIVE_ABI_FIELD(ArrowArrayStream, private_data)

SPARK_NATIVE_ABI_TYPE(SparkDsHost)
SPARK_NATIVE_ABI_FIELD(SparkDsHost, size)
SPARK_NATIVE_ABI_FIELD(SparkDsHost, abi_version)
SPARK_NATIVE_ABI_FIELD(SparkDsHost, spark_version)
SPARK_NATIVE_ABI_FIELD(SparkDsHost, log)
SPARK_NATIVE_ABI_FIELD(SparkDsHost, is_log_enabled)

SPARK_NATIVE_ABI_TYPE(SparkDsCreateInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsCreateInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsCreateInfo, name)
SPARK_NATIVE_ABI_FIELD(SparkDsCreateInfo, option_keys)
SPARK_NATIVE_ABI_FIELD(SparkDsCreateInfo, option_values)
SPARK_NATIVE_ABI_FIELD(SparkDsCreateInfo, num_options)

SPARK_NATIVE_ABI_TYPE(SparkDsReaderInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsReaderInfo, size)

SPARK_NATIVE_ABI_TYPE(SparkDsPartitionsInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsPartitionsInfo, size)

SPARK_NATIVE_ABI_TYPE(SparkDsStreamReaderInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsStreamReaderInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsStreamReaderInfo, checkpoint_location)

SPARK_NATIVE_ABI_TYPE(SparkDsInitialOffsetInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsInitialOffsetInfo, size)

SPARK_NATIVE_ABI_TYPE(SparkDsReadLimit)
SPARK_NATIVE_ABI_FIELD(SparkDsReadLimit, size)
SPARK_NATIVE_ABI_FIELD(SparkDsReadLimit, kind)
SPARK_NATIVE_ABI_FIELD(SparkDsReadLimit, max_rows)

SPARK_NATIVE_ABI_TYPE(SparkDsLatestOffsetInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsLatestOffsetInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsLatestOffsetInfo, start)
SPARK_NATIVE_ABI_FIELD(SparkDsLatestOffsetInfo, limit)
SPARK_NATIVE_ABI_FIELD(SparkDsLatestOffsetInfo, run_end)

SPARK_NATIVE_ABI_TYPE(SparkDsStreamPartitionsInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsStreamPartitionsInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsStreamPartitionsInfo, start)
SPARK_NATIVE_ABI_FIELD(SparkDsStreamPartitionsInfo, end)

SPARK_NATIVE_ABI_TYPE(SparkDsCommitOffsetInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsCommitOffsetInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsCommitOffsetInfo, end)

SPARK_NATIVE_ABI_TYPE(SparkDsTaskContext)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, size)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, stage_id)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, stage_attempt_number)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, partition_id)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, attempt_number)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, task_attempt_id)
SPARK_NATIVE_ABI_FIELD(SparkDsTaskContext, is_interrupted)

SPARK_NATIVE_ABI_TYPE(SparkDsReadInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, kind)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, schema)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, reader_state)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, reader_state_length)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, partition)
SPARK_NATIVE_ABI_FIELD(SparkDsReadInfo, partition_length)

SPARK_NATIVE_ABI_TYPE(SparkDsWriteInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsWriteInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsWriteInfo, mode)
SPARK_NATIVE_ABI_FIELD(SparkDsWriteInfo, query_id)

SPARK_NATIVE_ABI_TYPE(SparkDsDataWriterInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsDataWriterInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsDataWriterInfo, schema)
SPARK_NATIVE_ABI_FIELD(SparkDsDataWriterInfo, writer_state)
SPARK_NATIVE_ABI_FIELD(SparkDsDataWriterInfo, writer_state_length)
SPARK_NATIVE_ABI_FIELD(SparkDsDataWriterInfo, epoch_id)

SPARK_NATIVE_ABI_TYPE(SparkDsFinishWriteInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, size)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, epoch_id)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, messages)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, message_lengths)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, num_messages)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishWriteInfo, rejected)

SPARK_NATIVE_ABI_TYPE(SparkDsFinishDataWriterInfo)
SPARK_NATIVE_ABI_FIELD(SparkDsFinishDataWriterInfo, size)

SPARK_NATIVE_ABI_TYPE(SparkDsLibrary)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, size)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, abi_version)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, last_error)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, create)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, schema)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, create_reader)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, create_stream_reader)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, create_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, close)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, push_limit)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, prune_columns)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, partitions)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, close_reader)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, initial_offset)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, latest_offset)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, stream_partitions)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, commit_offset)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, close_stream_reader)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, read)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, commit)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, abort)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, close_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, create_data_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, write)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, commit_data_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, abort_data_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, close_data_writer)
SPARK_NATIVE_ABI_FIELD(SparkDsLibrary, default_read_limit)
