/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Additional operations required of a RecordReader to participate in a join.
 */
public interface ComposableRecordReader<K extends WritableComparable,
                                 V extends Writable>
    extends RecordReader<K,V>, Comparable<ComposableRecordReader<K,?>> {

  /**
   * Return the position in the collector this class occupies.
   */
  int id();

  /**
   * Return the key this RecordReader would supply on a call to next(K,V)
   */
  K key();

  /**
   * Clone the key at the head of this RecordReader into the object provided.
   */
  void key(K key) throws IOException;

  /**
   * Returns true if the stream is not empty, but provides no guarantee that
   * a call to next(K,V) will succeed.
   */
  boolean hasNext();

  /**
   * Skip key-value pairs with keys less than or equal to the key provided.
   */
  void skip(K key) throws IOException;

  /**
   * While key-value pairs from this RecordReader match the given key, register
   * them with the JoinCollector provided.
   */
  void accept(CompositeRecordReader.JoinCollector jc, K key) throws IOException;
}
