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

package org.apache.spark.sql.connector.catalog.index;

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.analysis.IndexAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchIndexException;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * Table methods for working with index
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsIndex extends Table {

  /**
   * A reserved property to specify the index type.
   */
  String PROP_TYPE = "type";

  /**
   * Creates an index.
   *
   * @param indexName the name of the index to be created
   * @param columns the columns on which index to be created
   * @param columnsProperties the properties of the columns on which index to be created
   * @param properties the properties of the index to be created
   * @throws IndexAlreadyExistsException If the index already exists.
   */
  void createIndex(String indexName,
      NamedReference[] columns,
      Map<NamedReference, Map<String, String>> columnsProperties,
      Map<String, String> properties)
      throws IndexAlreadyExistsException;

  /**
   * Drops the index with the given name.
   *
   * @param indexName the name of the index to be dropped.
   * @throws NoSuchIndexException If the index does not exist.
   */
  void dropIndex(String indexName) throws NoSuchIndexException;

  /**
   * Checks whether an index exists in this table.
   *
   * @param indexName the name of the index
   * @return true if the index exists, false otherwise
   */
  boolean indexExists(String indexName);

  /**
   * Lists all the indexes in this table.
   */
  TableIndex[] listIndexes();
}
