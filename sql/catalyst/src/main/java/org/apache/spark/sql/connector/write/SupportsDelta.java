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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A mix-in interface for {@link RowLevelOperation}. Data sources can implement this interface
 * to indicate they support handling deltas of rows.
 *
 * @since 3.4.0
 */
@Experimental
public interface SupportsDelta extends RowLevelOperation {
  @Override
  DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info);

  /**
   * Returns the row ID column references that should be used for row equality.
   */
  NamedReference[] rowId();

  /**
   * Controls whether to represent updates as deletes and inserts.
   * <p>
   * Data sources may choose to split updates into deletes and inserts to either better cluster
   * and order the incoming delta of rows or to simplify the write process.
   */
  default boolean representUpdateAsDeleteAndInsert() {
    return false;
  }
}
