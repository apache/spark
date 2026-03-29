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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Evolving;

/**
 * Write builder trait for tables that support dynamic partition overwrite.
 * <p>
 * A write that dynamically overwrites partitions removes all existing data in each logical
 * partition for which the write will commit new data. Any existing logical partition for which the
 * write does not contain data will remain unchanged.
 * <p>
 * This is provided to implement SQL compatible with Hive table operations but is not recommended.
 * Instead, use the {@link SupportsOverwriteV2 overwrite by filter API} to explicitly replace data.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsDynamicOverwrite extends WriteBuilder {
  /**
   * Configures a write to dynamically replace partitions with data committed in the write.
   *
   * @return this write builder for method chaining
   */
  WriteBuilder overwriteDynamicPartitions();
}
