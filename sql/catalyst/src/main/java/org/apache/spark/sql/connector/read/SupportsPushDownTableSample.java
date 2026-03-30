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
 * A mix-in interface for {@link Scan}. Data sources can implement this interface to
 * push down SAMPLE.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsPushDownTableSample extends ScanBuilder {

  /**
   * Pushes down SAMPLE to the data source.
   */
  boolean pushTableSample(
      double lowerBound,
      double upperBound,
      boolean withReplacement,
      long seed);

  /**
   * Pushes down SAMPLE to the data source with sample method awareness.
   * Data sources can override this to distinguish SYSTEM (block) from BERNOULLI (row) sampling.
   * By default, rejects SYSTEM sampling for backward compatibility and delegates BERNOULLI to
   *   the 4-parameter version.
   */
  default boolean pushTableSample(
      double lowerBound,
      double upperBound,
      boolean withReplacement,
      long seed,
      boolean isSystemSampling) {
    if (isSystemSampling) {
      // If the data source hasn't overridden this method, it must have not added support
      // for SYSTEM sampling. Don't apply sample pushdown.
      return false;
    }
    return pushTableSample(lowerBound, upperBound, withReplacement, seed);
  }
}
