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

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * An interface that carries query specific information for the data scanning job, like operator
 * pushdown information and streaming query offsets. This is defined as an empty interface, and data
 * sources should define their own {@link ScanConfig} classes.
 *
 * For APIs that take a {@link ScanConfig} as input, like
 * {@link ReadSupport#planInputPartitions(ScanConfig)},
 * {@link BatchReadSupport#createReaderFactory(ScanConfig)} and
 * {@link SupportsReportStatistics#estimateStatistics(ScanConfig)}, implementations mostly need to
 * cast the input {@link ScanConfig} to the concrete {@link ScanConfig} class of the data source.
 */
@Evolving
public interface ScanConfig {

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  StructType readSchema();
}
