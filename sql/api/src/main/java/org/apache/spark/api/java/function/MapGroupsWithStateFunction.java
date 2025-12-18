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

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.streaming.GroupState;

/**
 * ::Experimental::
 * Base interface for a map function used in
 * {@link org.apache.spark.sql.KeyValueGroupedDataset#mapGroupsWithState(
 * MapGroupsWithStateFunction, org.apache.spark.sql.Encoder, org.apache.spark.sql.Encoder)}
 * @since 2.1.1
 */
@Evolving
public interface MapGroupsWithStateFunction<K, V, S, R> extends Serializable {
  R call(K key, Iterator<V> values, GroupState<S> state) throws Exception;
}
