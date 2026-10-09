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

package org.apache.spark.sql.datasource;

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.columnar.ColumnarTable;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The entry point of a {@link DataSource} implemented on the JVM.
 * <p>
 * Register a provider like any other data source: list the class in
 * {@code META-INF/services/org.apache.spark.sql.sources.DataSourceRegister} to use it by its
 * {@link #shortName()}, for example {@code spark.read.format("my_source")}, or use its fully
 * qualified class name as the format.
 *
 * @since 4.4.0
 */
@Evolving
public abstract class DataSourceProvider implements TableProvider, DataSourceRegister {

  /**
   * Creates the data source with the options given by the user, such as
   * {@code spark.read.option("key", "value")}.
   */
  public abstract DataSource createDataSource(CaseInsensitiveStringMap options);

  @Override
  public final StructType inferSchema(CaseInsensitiveStringMap options) {
    return createDataSource(options).schema();
  }

  @Override
  public final Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new ColumnarTable(shortName(), schema, this::createDataSource);
  }

  @Override
  public final boolean supportsExternalMetadata() {
    return true;
  }
}
