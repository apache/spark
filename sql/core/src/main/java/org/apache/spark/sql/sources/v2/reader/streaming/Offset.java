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

/**
 * An abstract representation of progress through a {@link MicroBatchReader} or
 * {@link ContinuousReader}.
 * During execution, offsets provided by the data source implementation will be logged and used as
 * restart checkpoints. Each source should provide an offset implementation which the source can use
 * to reconstruct a position in the stream up to which data has been seen/processed.
 *
 * Note: This class currently extends {@link org.apache.spark.sql.execution.streaming.Offset} to
 * maintain compatibility with DataSource V1 APIs. This extension will be removed once we
 * get rid of V1 completely.
 */
@InterfaceStability.Evolving
public abstract class Offset extends org.apache.spark.sql.execution.streaming.Offset {
    /**
     * A JSON-serialized representation of an Offset that is
     * used for saving offsets to the offset log.
     * Note: We assume that equivalent/equal offsets serialize to
     * identical JSON strings.
     *
     * @return JSON string encoding
     */
    public abstract String json();

    /**
     * Equality based on JSON string representation. We leverage the
     * JSON representation for normalization between the Offset's
     * in deserialized and serialized representations.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof org.apache.spark.sql.execution.streaming.Offset) {
            return this.json()
                .equals(((org.apache.spark.sql.execution.streaming.Offset) obj).json());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.json().hashCode();
    }

    @Override
    public String toString() {
        return this.json();
    }
}
