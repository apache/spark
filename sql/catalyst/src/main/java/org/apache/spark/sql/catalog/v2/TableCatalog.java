/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface TableCatalog extends CatalogProvider {
  /**
   * Load table metadata by {@link TableIdentifier identifier} from the catalog.
   *
   * @param ident a table identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist.
   */
  Table loadTable(TableIdentifier ident) throws NoSuchTableException;

  /**
   * Test whether a table exists using an {@link TableIdentifier identifier} from the catalog.
   *
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(TableIdentifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table already exists for the identifier
   */
  default Table createTable(TableIdentifier ident,
                            StructType schema) throws TableAlreadyExistsException {
    return createTable(ident, schema, Collections.emptyList(), Collections.emptyMap());
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table already exists for the identifier
   */
  default Table createTable(TableIdentifier ident,
                            StructType schema,
                            Map<String, String> properties) throws TableAlreadyExistsException {
    return createTable(ident, schema, Collections.emptyList(), properties);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions a list of expressions to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table already exists for the identifier
   */
  Table createTable(TableIdentifier ident,
                    StructType schema,
                    List<Expression> partitions,
                    Map<String, String> properties) throws TableAlreadyExistsException;

  /**
   * Apply a list of {@link TableChange changes} to a table in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the table.
   *
   * @param ident a table identifier
   * @param changes a list of changes to apply to the table
   * @return updated metadata for the table
   * @throws NoSuchTableException If the table doesn't exist.
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  Table alterTable(TableIdentifier ident,
                   List<TableChange> changes) throws NoSuchTableException;

  /**
   * Apply {@link TableChange changes} to a table in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the table.
   *
   * @param ident a table identifier
   * @param changes a list of changes to apply to the table
   * @return updated metadata for the table
   * @throws NoSuchTableException If the table doesn't exist.
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  default Table alterTable(TableIdentifier ident,
                           TableChange... changes) throws NoSuchTableException {
    return alterTable(ident, Arrays.asList(changes));
  }

  /**
   * Drop a table in the catalog.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  boolean dropTable(TableIdentifier ident);
}
