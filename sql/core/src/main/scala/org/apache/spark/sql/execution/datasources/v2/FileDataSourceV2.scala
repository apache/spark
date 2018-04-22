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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.ReadSupport
import org.apache.spark.sql.sources.v2.WriteSupport

/**
 * The base class for file data source v2. Implementations must have a public, 0-arg constructor.
 *
 * Note that this is an empty interface. Data source implementations should mix-in at least one of
 * the plug-in interfaces like {@link ReadSupport} and {@link WriteSupport}. Otherwise it's just
 * a dummy data source which is un-readable/writable.
 */
trait FileDataSourceV2 extends DataSourceV2 with DataSourceRegister {
  /**
   * Returns an optional V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 might be implemented partially during migration.
   *    E.g. if [[ReadSupport]] is implemented while [[WriteSupport]] is not,
   *    write path should fall back to V1 implementation.
   * 2. File datasource V2 implementations cause regression.
   * 3. Catalog support is required, which is still under development for data source V2.
   */
  def fallBackFileFormat: Option[Class[_]] = None
}
