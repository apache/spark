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

package org.apache.spark.util.kvstore;

import org.apache.spark.annotation.Private;

/**
 * Creates compact records whose indexed fields can be read without decoding the full value.
 * Implementations may share storage between records, for example a compressed task block.
 */
@Private
public interface KVStoreRecordCodec<T> {

  Record<T> encode(T value) throws Exception;

  /** Seals any partially filled blocks without changing the values of existing records. */
  default void compact() {}

  /**
   * An immutable value snapshot. A record may outlive its membership in the store because a
   * concurrent iterator can still hold it. Neither compaction nor removal may change its value.
   */
  interface Record<T> {
    T decode() throws Exception;

    /** Returns the fields needed by list pages; ordinary reads always use {@link #decode()}. */
    default T decodeSummary() throws Exception {
      return decode();
    }

    Object indexValue(String indexName) throws Exception;

    /**
     * Releases any codec-owned reference when this record is replaced or removed. This callback
     * must not invalidate the record or its indexed values, including after the store is closed.
     */
    default void removed() {}
  }
}
