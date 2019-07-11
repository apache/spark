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

package org.apache.spark.sql.sources.v2.maintain;

import org.apache.spark.annotation.Evolving;

/**
 * An interface for build {@link Maintainer}.
 */
@Evolving
public interface MaintainerBuilder {

  /**
   * Passes the `queryId` from Spark to data source. `queryId` is a unique string of the query.
   * Some datasource may use this id to identify queries.
   *
   * @return a new builder with the `queryId`. By default it returns `this`, which means the given
   *         `queryId` is ignored. Please override this method to take the `queryId`.
   */
  default MaintainerBuilder withQueryId(String queryId) {
    return this;
  }

  Maintainer build();
}
