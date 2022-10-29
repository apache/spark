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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down all the join or aggregate keys to data sources. A return value true indicates
 * that data source will return input partitions (via planInputPartitions} following the
 * clustering keys. Otherwise, a false return value indicates the data source doesn't make
 * such a guarantee, even though it may still report a partitioning that may or may not
 * be compatible with the given clustering keys, and it's Spark's responsibility to group
 * the input partitions whether it can be applied.
 *
 * @since 3.4.0
 */
@Evolving
public interface SupportsPushDownClusterKeys extends ScanBuilder {

  /**
   * Pushes down cluster keys to the data source.
   */
  boolean pushClusterKeys(Expression[] expressions);
}
