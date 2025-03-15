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

package org.apache.spark.sql.connector.catalog;

import com.google.common.collect.Maps;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;

import java.util.Map;

/**
 * Builder for creating tables. Spark gets the builder from the {@link TableCatalog},
 * calls the `withXXX` methods to set up the fields and calls the `create` method.
 */
public class TableBuilder {
  protected final TableCatalog catalog;

  protected final Identifier identifier;

  protected final Column[] columns;

  protected final Map<String, String> properties = Maps.newHashMap();

  protected Transform[] partitions = new Transform[0];

  /**
   * Constructor for TableBuilder.
   *
   * @param catalog catalog where table needs to be created.
   * @param identifier identifier for the table.
   * @param columns the columns of the new table.
   */
  public TableBuilder(TableCatalog catalog, Identifier identifier, Column[] columns) {
    this.catalog = catalog;
    this.identifier = identifier;
    this.columns = columns;
  }

  /**
   * Sets the partitions for the table.
   *
   * @param partitions Partitions for the table.
   * @return this for method chaining
   */
  public TableBuilder withPartitions(Transform[] partitions) {
    this.partitions = partitions;
    return this;
  }

  /**
   * Sets the key/value properties to the table.
   *
   * @param properties key/value properties
   * @return this for method chaining
   */
  public TableBuilder withProperties(Map<String, String> properties) {
    this.properties.clear();
    this.properties.putAll(properties);
    return this;
  }

  /**
   * Creates the table.
   *
   * @return metadata for the new table. This can be null if getting the metadata for the new
   * table is expensive. Spark will call {@link #loadTable(Identifier)} if needed (e.g. CTAS).
   *
   * @throws TableAlreadyExistsException   If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException      If the identifier namespace does not exist (optional)
   */
  public Table create() throws TableAlreadyExistsException, NoSuchNamespaceException {
    return catalog.createTable(identifier, columns, partitions, properties);
  }
}
