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
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to
 * report data partitioning and try to avoid shuffle at Spark side.
 *
 * Note that, when a {@link Scan} implementation creates exactly one {@link InputPartition},
 * Spark may avoid adding a shuffle even if the reader does not implement this interface.
 */
@Evolving
public interface SupportsReportPartitioning extends Scan {

  /**
   * Returns the output data partitioning that this reader guarantees.
   */
  Partitioning outputPartitioning();
}
