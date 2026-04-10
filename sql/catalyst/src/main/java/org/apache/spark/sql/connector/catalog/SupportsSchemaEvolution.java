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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;

/**
 * A mix-in interface for {@link Table} schema evolution support. Data sources can implement this
 * interface to indicate the schema changes they support during write operations. Tables must
 * report capability {@link TableCapability#AUTOMATIC_SCHEMA_EVOLUTION} for schema evolution to
 * happen.
 * <p>
 * During automatic schema evolution, Spark computes the set of column changes (e.g. adding a new
 * column, widening a column type) needed to make the target table schema compatible with the
 * source data schema. Each candidate change is passed to {@link #supportsColumnChange} to
 * determine whether the data source can apply it. Changes that are not supported are skipped and
 * Spark will attempt to resolve such changes using casts. If casting is not supported, the query
 * will fail.
 *
 * @since 4.2.0
 */
@Experimental
public interface SupportsSchemaEvolution extends Table {

  /**
   * Returns whether the given column change can be applied to the table during automatic schema
   * evolution. This should return {@code true} when the data source can physically apply the
   * change, for example adding a new column or performing a lossless type widening.
   */
  boolean supportsColumnChange(ColumnChange change);
}
