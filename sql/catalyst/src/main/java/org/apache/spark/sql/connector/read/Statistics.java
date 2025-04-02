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

package org.apache.spark.sql.connector.read;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;

/**
 * An interface to represent statistics for a data source, which is returned by
 * {@link SupportsReportStatistics#estimateStatistics()}.
 *
 * @since 3.0.0
 */
@Evolving
public interface Statistics {
  OptionalLong sizeInBytes();
  OptionalLong numRows();
  default Map<NamedReference, ColumnStatistics> columnStats() {
    return new HashMap<>();
  }
}
