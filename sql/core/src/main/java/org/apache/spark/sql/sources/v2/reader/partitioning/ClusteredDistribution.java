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

package org.apache.spark.sql.sources.v2.reader.partitioning;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

/**
 * A concrete implementation of {@link Distribution}. Represents a distribution where records that
 * share the same values for the {@link #clusteredColumns} will be produced by the same
 * {@link InputPartitionReader}.
 */
@InterfaceStability.Evolving
public class ClusteredDistribution implements Distribution {

  /**
   * The names of the clustered columns. Note that they are order insensitive.
   */
  public final String[] clusteredColumns;

  public ClusteredDistribution(String[] clusteredColumns) {
    this.clusteredColumns = clusteredColumns;
  }
}
