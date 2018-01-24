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

package org.apache.spark.sql.sources.v2.reader;

import java.util.List;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to output {@link UnsafeRow} directly and avoid the row copy at Spark side.
 * This is an experimental and unstable interface, as {@link UnsafeRow} is not public and may get
 * changed in the future Spark versions.
 */
@InterfaceStability.Unstable
public interface SupportsScanUnsafeRow extends DataSourceV2Reader {

  @Override
  default List<ReadTask<Row>> createReadTasks() {
    throw new IllegalStateException(
      "createReadTasks not supported by default within SupportsScanUnsafeRow");
  }

  /**
   * Similar to {@link DataSourceV2Reader#createReadTasks()}, but returns data in unsafe row format.
   */
  List<ReadTask<UnsafeRow>> createUnsafeRowReadTasks();
}
