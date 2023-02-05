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

package org.apache.spark.network.shuffledb;

import java.io.Closeable;

import org.apache.spark.annotation.Private;

/**
 * The local KV storage used to persist the shuffle state,
 * the implementations may include LevelDB, RocksDB, etc.
 */
@Private
public interface DB extends Closeable {
    /**
     * Set the DB entry for "key" to "value".
     */
    void put(byte[] key, byte[] value);

    /**
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.
     */
    byte[] get(byte[] key);

    /**
     * Delete the DB entry (if any) for "key".
     */
    void delete(byte[] key);

    /**
     * Return an iterator over the contents of the DB.
     */
    DBIterator iterator();
}
