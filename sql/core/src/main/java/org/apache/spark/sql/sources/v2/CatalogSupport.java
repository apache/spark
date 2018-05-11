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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.catalog.DataSourceCatalog;

/**
 * A mix-in interface for {@link DataSourceV2} catalog support. Data sources can implement this
 * interface to provide the ability to load, create, alter, and drop tables.
 * <p>
 * Data sources must implement this interface to support logical operations that combine writing
 * data with catalog tasks, like create-table-as-select.
 */
public interface CatalogSupport {
  /**
   * Return a {@link DataSourceCatalog catalog} for tables.
   *
   * @return a catalog
   */
  DataSourceCatalog catalog();
}
