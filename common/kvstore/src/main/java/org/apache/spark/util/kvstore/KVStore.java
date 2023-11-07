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

import java.io.Closeable;
import java.util.Collection;

import org.apache.spark.annotation.Private;

/**
 * Abstraction for a local key/value store for storing app data.
 *
 * <p>
 * There are two main features provided by the implementations of this interface:
 * </p>
 *
 * <h2>Serialization</h2>
 *
 * <p>
 * If the underlying data store requires serialization, data will be serialized to and deserialized
 * using a {@link KVStoreSerializer}, which can be customized by the application. The serializer is
 * based on Jackson, so it supports all the Jackson annotations for controlling the serialization of
 * app-defined types.
 * </p>
 *
 * <p>
 * Data is also automatically compressed to save disk space.
 * </p>
 *
 * <h2>Automatic Key Management</h2>
 *
 * <p>
 * When using the built-in key management, the implementation will automatically create unique
 * keys for each type written to the store. Keys are based on the type name, and always start
 * with the "+" prefix character (so that it's easy to use both manual and automatic key
 * management APIs without conflicts).
 * </p>
 *
 * <p>
 * Another feature of automatic key management is indexing; by annotating fields or methods of
 * objects written to the store with {@link KVIndex}, indices are created to sort the data
 * by the values of those properties. This makes it possible to provide sorting without having
 * to load all instances of those types from the store.
 * </p>
 *
 * <p>
 * KVStore instances are thread-safe for both reads and writes.
 * </p>
 */
@Private
public interface KVStore extends Closeable {

  /**
   * Returns app-specific metadata from the store, or null if it's not currently set.
   *
   * <p>
   * The metadata type is application-specific. This is a convenience method so that applications
   * don't need to define their own keys for this information.
   * </p>
   */
  <T> T getMetadata(Class<T> klass) throws Exception;

  /**
   * Writes the given value in the store metadata key.
   */
  void setMetadata(Object value) throws Exception;

  /**
   * Read a specific instance of an object.
   *
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */
  <T> T read(Class<T> klass, Object naturalKey) throws Exception;

  /**
   * Writes the given object to the store, including indexed fields. Indices are updated based
   * on the annotated fields of the object's class.
   *
   * <p>
   * Writes may be slower when the object already exists in the store, since it will involve
   * updating existing indices.
   * </p>
   *
   * @param value The object to write.
   */
  void write(Object value) throws Exception;

  /**
   * Removes an object and all data related to it, like index entries, from the store.
   *
   * @param type The object's type.
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */
  void delete(Class<?> type, Object naturalKey) throws Exception;

  /**
   * Returns a configurable view for iterating over entities of the given type.
   */
  <T> KVStoreView<T> view(Class<T> type) throws Exception;

  /**
   * Returns the number of items of the given type currently in the store.
   */
  long count(Class<?> type) throws Exception;

  /**
   * Returns the number of items of the given type which match the given indexed value.
   */
  long count(Class<?> type, String index, Object indexedValue) throws Exception;

  /**
   * A cheaper way to remove multiple items from the KVStore
   */
  <T> boolean removeAllByIndexValues(Class<T> klass, String index, Collection<?> indexValues)
      throws Exception;
}
