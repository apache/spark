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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A marker interface to provide a catalog implementation for Spark.
 * <p>
 * Implementations can provide catalog functions by implementing additional interfaces for tables,
 * views, and functions.
 * <p>
 * Catalog implementations must implement this marker interface to be loaded by
 * {@link Catalogs#load(String, SQLConf)}. The loader will instantiate catalog classes using the
 * required public no-arg constructor. After creating an instance, it will be configured by calling
 * {@link #initialize(String, CaseInsensitiveStringMap)}.
 * <p>
 * Catalog implementations are registered to a name by adding a configuration option to Spark:
 * {@code spark.sql.catalog.catalog-name=com.example.YourCatalogClass}. All configuration properties
 * in the Spark configuration that share the catalog name prefix,
 * {@code spark.sql.catalog.catalog-name.(key)=(value)} will be passed in the case insensitive
 * string map of options in initialization with the prefix removed.
 * {@code name}, is also passed and is the catalog's name; in this case, "catalog-name".
 */
@Experimental
public interface CatalogPlugin {
  /**
   * Called to initialize configuration.
   * <p>
   * This method is called once, just after the provider is instantiated.
   *
   * @param name the name used to identify and load this catalog
   * @param options a case-insensitive string map of configuration
   */
  void initialize(String name, CaseInsensitiveStringMap options);

  /**
   * Called to get this catalog's name.
   * <p>
   * This method is only called after {@link #initialize(String, CaseInsensitiveStringMap)} is
   * called to pass the catalog's name.
   */
  String name();
}
