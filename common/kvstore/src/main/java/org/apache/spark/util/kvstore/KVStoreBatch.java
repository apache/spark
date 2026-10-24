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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** Values and index counts written earlier in a disk batch, which are not visible in the DB yet. */
class KVStoreBatch {
  private final Map<ByteBuffer, Object> values = new HashMap<>();
  private final Map<ByteBuffer, Long> counts = new HashMap<>();

  Object put(byte[] key, Object value) {
    return values.put(ByteBuffer.wrap(key), value);
  }

  Long count(byte[] key) {
    return counts.get(ByteBuffer.wrap(key));
  }

  void count(byte[] key, long value) {
    counts.put(ByteBuffer.wrap(key), value);
  }
}
