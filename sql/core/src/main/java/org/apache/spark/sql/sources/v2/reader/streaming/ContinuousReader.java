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

package org.apache.spark.sql.sources.v2.reader.streaming;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.BaseStreamingSource;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

import java.util.Optional;

/**
 * A mix-in interface for {@link DataSourceReader}. Data source readers can implement this
 * interface to allow reading in a continuous processing mode stream.
 *
 * Implementations must ensure each partition reader is a {@link ContinuousInputPartitionReader}.
 *
 * Note: This class currently extends {@link BaseStreamingSource} to maintain compatibility with
 * DataSource V1 APIs. This extension will be removed once we get rid of V1 completely.
 */
@InterfaceStability.Evolving
public interface ContinuousReader extends BaseStreamingSource, DataSourceReader {
    /**
     * Merge partitioned offsets coming from {@link ContinuousInputPartitionReader} instances
     * for each partition to a single global offset.
     */
    Offset mergeOffsets(PartitionOffset[] offsets);

    /**
     * Deserialize a JSON string into an Offset of the implementation-defined offset type.
     * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
     */
    Offset deserializeOffset(String json);

    /**
     * Set the desired start offset for partitions created from this reader. The scan will
     * start from the first record after the provided offset, or from an implementation-defined
     * inferred starting point if no offset is provided.
     */
    void setStartOffset(Optional<Offset> start);

    /**
     * Return the specified or inferred start offset for this reader.
     *
     * @throws IllegalStateException if setStartOffset has not been called
     */
    Offset getStartOffset();

    /**
     * The execution engine will call this method in every epoch to determine if new input
     * partitions need to be generated, which may be required if for example the underlying
     * source system has had partitions added or removed.
     *
     * If true, the query will be shut down and restarted with a new reader.
     */
    default boolean needsReconfiguration() {
        return false;
    }

    /**
     * Informs the source that Spark has completed processing all data for offsets less than or
     * equal to `end` and will only request offsets greater than `end` in the future.
     */
    void commit(Offset end);
}
