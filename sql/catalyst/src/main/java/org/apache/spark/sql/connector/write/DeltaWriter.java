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

package org.apache.spark.sql.connector.write;

import java.io.IOException;

import org.apache.spark.annotation.Experimental;

/**
 * A data writer returned by {@link DeltaWriterFactory#createWriter(int, long)} and is
 * responsible for writing a delta of rows.
 *
 * @since 3.4.0
 */
@Experimental
public interface DeltaWriter<T> extends DataWriter<T> {
  /**
   * Deletes a row.
   *
   * @param metadata values for metadata columns that were projected but are not part of the row ID
   * @param id a row ID to delete
   * @throws IOException if failure happens during disk/network IO like writing files
   */
  void delete(T metadata, T id) throws IOException;

  /**
   * Updates a row.
   *
   * @param metadata values for metadata columns that were projected but are not part of the row ID
   * @param id a row ID to update
   * @param row a row with updated values
   * @throws IOException if failure happens during disk/network IO like writing files
   */
  void update(T metadata, T id, T row) throws IOException;

  /**
   * Inserts a new row.
   *
   * @param row a row to insert
   * @throws IOException if failure happens during disk/network IO like writing files
   */
  void insert(T row) throws IOException;

  @Override
  default void write(T row) throws IOException {
    insert(row);
  }
}
