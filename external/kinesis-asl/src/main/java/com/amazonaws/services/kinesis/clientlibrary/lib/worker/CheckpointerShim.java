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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;


/**
 * This is a class to fix an issue in SPARK-18020. When resharding Kinesis streams,
 * the KCL throws an exception because Spark does not checkpoint `SHARD_END` to finish reading
 * closed shards in `KinesisRecordProcessor#shutdown`. This bug finally leads to stopping
 * subscribing new split (or merged) shards. However, trying checkpoints with `SHARD_END` throws
 * `IllegalArgumentException` exceptions with messages like "Sequence number must be numeric,
 * but was SHARD_END". This fix is a workaround and the class will be removed in future
 * if we find other better solutions.
 */
public class CheckpointerShim {

  public static void shutdown(IRecordProcessorCheckpointer checkpointer)
      throws InvalidStateException, ShutdownException{
    if (checkpointer != null && checkpointer instanceof RecordProcessorCheckpointer) {
      RecordProcessorCheckpointer finalCheckpointer = (RecordProcessorCheckpointer) checkpointer;
      finalCheckpointer.advancePosition(ExtendedSequenceNumber.SHARD_END);
    }
  }
}
