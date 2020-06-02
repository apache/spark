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
import org.apache.spark.sql.sources.AlwaysTrue$;
import org.apache.spark.sql.sources.Filter;

/**
 * Write builder trait for tables that support overwrite by filter.
 * <p>
 * Overwriting data by filter will delete any data that matches the filter and replace it with data
 * that is committed in the write.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsOverwrite extends WriteBuilder, SupportsTruncate {
  /**
   * Configures a write to replace data matching the filters with data committed in the write.
   * <p>
   * Rows must be deleted from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   *
   * @param filters filters used to match data to overwrite
   * @return this write builder for method chaining
   */
  WriteBuilder overwrite(Filter[] filters);

  @Override
  default WriteBuilder truncate() {
    return overwrite(new Filter[] { AlwaysTrue$.MODULE$ });
  }
}
