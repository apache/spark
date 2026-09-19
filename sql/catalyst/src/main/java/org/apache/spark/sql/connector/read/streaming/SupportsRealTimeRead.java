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
 * <p>
 * <b>Which method to implement.</b> A source reader must provide a {@code nextWithTimeout} that
 * proceeds to the next record, blocking until one is available or a timeout elapses. There are two
 * overloads, and a source should override exactly one:
 * <ul>
 *   <li>{@link #nextWithTimeout(Long)} -- implement this in the common case. It receives only the
 *       timeout and is all a third-party source needs.</li>
 *   <li>{@link #nextWithTimeout(Long, Long)} -- the overload the execution engine actually invokes.
 *       Its default implementation ignores the extra parameter and delegates to
 *       {@link #nextWithTimeout(Long)}. Override it only for engine-internal sources that must
 *       observe the engine's reference start time (used together with the engine-internal low
 *       latency clock); that clock is not part of the third-party contract, so most sources should
 *       not override this overload.</li>
 * </ul>
 * Because both overloads are {@code default} methods, overriding neither is not caught at compile
 * time -- it fails at runtime when {@link #nextWithTimeout(Long)} throws.
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
     * <p>
     * This is the recommended method to implement for a source. It is enough for any source that
     * does not need the engine's reference start time (see {@link #nextWithTimeout(Long, Long)}).
     * The engine always invokes {@link #nextWithTimeout(Long, Long)}, whose default implementation
     * delegates here, so overriding only this method is sufficient. If a source overrides neither
     * overload, this default implementation throws to surface the mistake.
     * @param timeoutMs if no result is available after this timeout (milliseconds), return
     * @return {@link RecordStatus} describing whether a record is available and its arrival time
     * @throws IOException
     */
    default RecordStatus nextWithTimeout(Long timeoutMs) throws IOException {
        throw new UnsupportedOperationException(
            "A SupportsRealTimeRead implementation must override either " +
            "nextWithTimeout(Long) or nextWithTimeout(Long, Long).");
    }

    /**
     * The overload of {@link #nextWithTimeout(Long)} that the execution engine actually invokes. In
     * addition to the timeout it receives {@code startTimeMs}, the reference time the engine used
     * to compute that timeout, so the engine and the source agree on when the wait started (this
     * matters when the engine runs against its internal low latency clock, e.g. a manual clock in
     * tests). The default implementation ignores {@code startTimeMs} and delegates to
     * {@link #nextWithTimeout(Long)}.
     * <p>
     * This is intended for engine-internal sources. The reference clock is not part of the
     * third-party contract, so third-party sources should implement {@link #nextWithTimeout(Long)}
     * and leave this overload to its default.
     * @param startTimeMs the base time (milliseconds) that was used to calculate the timeout
     * @param timeoutMs if no result is available after this timeout (milliseconds), return
     * @return {@link RecordStatus} describing whether a record is available and its arrival time
     * @throws IOException
     */
    default RecordStatus nextWithTimeout(Long startTimeMs, Long timeoutMs) throws IOException {
        return nextWithTimeout(timeoutMs);
    }
}
