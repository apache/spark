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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link TableProvider}. Data sources can implement this interface to
 * propagate session configs with the specified key-prefix to all data source operations in this
 * session.
 *
 * @since 3.0.0
 */
@Evolving
public interface SessionConfigSupport extends TableProvider {

  /**
   * Key prefix of the session configs to propagate, which is usually the data source name. Spark
   * will extract all session configs that starts with `spark.datasource.$keyPrefix`, turn
   * `spark.datasource.$keyPrefix.xxx -&gt; yyy` into `xxx -&gt; yyy`, and propagate them to all
   * data source operations in this session.
   */
  String keyPrefix();
}
