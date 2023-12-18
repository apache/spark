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

import org.apache.spark.annotation.Evolving;

/**
 * An abstract representation of progress through a {@link MicroBatchStream} or
 * {@link ContinuousStream}.
 * <p>
 * During execution, offsets provided by the data source implementation will be logged and used as
 * restart checkpoints. Each source should provide an offset implementation which the source can use
 * to reconstruct a position in the stream up to which data has been seen/processed.
 *
 * @since 3.0.0
 */
@Evolving
public abstract class Offset {
    /**
     * A JSON-serialized representation of an Offset that is
     * used for saving offsets to the offset log.
     * <p>
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
        if (obj instanceof Offset offset) {
            return this.json().equals(offset.json());
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
