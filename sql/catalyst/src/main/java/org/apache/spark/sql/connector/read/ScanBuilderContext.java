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
 * Planning context provided to a data source when creating a {@link ScanBuilder}.
 *
 * @since 4.4.0
 */
@Evolving
public class ScanBuilderContext {
  private final boolean isInSubquery;

  public ScanBuilderContext(boolean isInSubquery) {
    this.isInSubquery = isInSubquery;
  }

  /**
   * Returns whether Spark is constructing this scan within a subquery.
   *
   * <p>Data sources can use this context to adjust planning work, such as statistics computation.
   * It must not change the rows returned by the scan.
   */
  public boolean isInSubquery() {
    return isInSubquery;
  }
}
