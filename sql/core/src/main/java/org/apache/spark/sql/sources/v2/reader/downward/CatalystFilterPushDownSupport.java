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

package org.apache.spark.sql.sources.v2.reader.downward;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * A mix-in interface for `DataSourceV2Reader`. Users can implement this interface to push down
 * arbitrary expressions as predicates to the data source. This is an experimental and unstable
 * interface
 *
 * Note that, if users implement both this interface and `FilterPushDownSupport`, Spark will ignore
 * `FilterPushDownSupport` and only process this interface.
 */
@Experimental
@InterfaceStability.Unstable
public interface CatalystFilterPushDownSupport {

  /**
   * Pushes down filters, and returns unsupported filters.
   */
  Expression[] pushCatalystFilters(Expression[] filters);
}
