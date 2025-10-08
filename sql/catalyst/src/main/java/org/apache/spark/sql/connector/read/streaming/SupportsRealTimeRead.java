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

package org.apache.spark.sql.connector.read.streaming;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * A variation on {@link PartitionReader} for use with low latency streaming processing.
 *
 */
@Evolving
public interface SupportsRealTimeRead<T> extends PartitionReader<T> {

    /**
     * A class to represent the status of a record to be read as the return type of nextWithTimeout.
     * It contains whether the next record is available and the ingestion time of the record
     * if the source connector provided relevant info. A list of source connector that has ingestion
     * time is listed below:
     * - Kafka when the record timestamp type is LogAppendTime
     * - Kinesis has ApproximateArrivalTimestamp
     */
    class RecordStatus {
        private final boolean hasRecord;
        private final Optional<Long> recArrivalTime;

        private RecordStatus(boolean hasRecord, Optional<Long> recArrivalTime) {
            this.hasRecord = hasRecord;
            this.recArrivalTime = recArrivalTime;
        }

        // Public factory methods to control instance creation
        public static RecordStatus newStatusWithoutArrivalTime(boolean hasRecord) {
            return new RecordStatus(hasRecord, Optional.empty());
        }

        public static RecordStatus newStatusWithArrivalTimeMs(Long recArrivalTime) {
            return new RecordStatus(true, Optional.of(recArrivalTime));
        }

        public boolean hasRecord() {
            return hasRecord;
        }

        public Optional<Long> recArrivalTime() {
            return recArrivalTime;
        }
    }

    /**
     * Get the offset of the next record, or the start offset if no records have been read.
     * <p>
     * The execution engine will call this method along with get() to keep track of the current
     * offset. When a task ends, the offset in each partition will be passed back to the driver.
     * They will be used as the start offsets of the next batch.
     */
    PartitionOffset getOffset();

    /**
     * Alternative function to be called than next(), that proceed to the next record. The different
     * from next() is that, if there is no more records, the call needs to keep waiting until
     * the timeout.
     * @param timeout if no result is available after this timeout (milliseconds), return
     * @return {@link RecordStatus} describing whether a record is available and its arrival time
     * @throws IOException
     */
    RecordStatus nextWithTimeout(Long timeout) throws IOException;
}
