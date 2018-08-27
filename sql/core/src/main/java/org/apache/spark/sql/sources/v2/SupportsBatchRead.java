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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.reader.BatchScan;
import org.apache.spark.sql.sources.v2.reader.ScanConfig;
import org.apache.spark.sql.sources.v2.reader.ScanConfigBuilder;

/**
 * A mix-in interface for {@link Table}. Table implementations can mixin this interface to
 * provide data reading ability for batch processing.
 */
@InterfaceStability.Evolving
public interface SupportsBatchRead extends Table {

  /**
   * Creates a {@link BatchScan} instance with a {@link ScanConfig} and user-specified options.
   *
   * @param config a {@link ScanConfig} which may contains operator pushdown information.
   * @param options the user-specified options, which is same as the one used to create the
   *                {@link ScanConfigBuilder} that built the given {@link ScanConfig}.
   */
  BatchScan createBatchScan(ScanConfig config, DataSourceOptions options);
}
