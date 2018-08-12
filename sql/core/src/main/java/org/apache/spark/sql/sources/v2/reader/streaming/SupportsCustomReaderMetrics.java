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
package org.apache.spark.sql.sources.v2.reader.streaming;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.CustomMetrics;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/**
 * A mix in interface for {@link DataSourceReader}. Data source readers can implement this
 * interface to report custom metrics that gets reported under the
 * {@link org.apache.spark.sql.streaming.SourceProgress}
 *
 */
@InterfaceStability.Evolving
public interface SupportsCustomReaderMetrics extends DataSourceReader {
  /**
   * Returns custom metrics specific to this data source.
   */
  CustomMetrics getCustomMetrics();

  /**
   * Invoked if the custom metrics returned by {@link #getCustomMetrics()} is invalid
   * (e.g. Invalid data that cannot be parsed). Throwing an error here would ensure that
   * your custom metrics work right and correct values are reported always. The default action
   * on invalid metrics is to ignore it.
   *
   * @param ex the exception
   */
  default void onInvalidMetrics(Exception ex) {
    // default is to ignore invalid custom metrics
  }
}
