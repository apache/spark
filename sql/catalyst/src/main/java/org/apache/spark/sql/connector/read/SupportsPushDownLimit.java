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

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down LIMIT. We can push down LIMIT with many other operations if they follow the
 * operator order we defined in {@link ScanBuilder}'s class doc.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsPushDownLimit extends ScanBuilder {

  /**
   * Pushes down LIMIT to the data source.
   */
  boolean pushLimit(int limit);

  /**
   * Whether the LIMIT is partially pushed or not. If it returns true, then Spark will do LIMIT
   * again. This method will only be called when {@link #pushLimit} returns true.
   */
  default boolean isPartiallyPushed() { return true; }
}
