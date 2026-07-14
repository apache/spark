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
 * A mix-in interface for {@link RowLevelOperation}. Data sources can implement this interface to
 * receive a narrow row containing only the columns declared via {@link #requiredDataAttributes()}
 * for updated, copied, and reinserted records, instead of the full table row.
 *
 * @since 4.3.0
 */
@Experimental
public interface SupportsColumnUpdates extends RowLevelOperation {
  /**
   * Returns the data column references required to perform this row-level operation.
   * <p>
   * The returned columns become the schema of updated, copied, and reinserted rows, in declared
   * order. Implementations must include every column they want to receive (typically the columns
   * reported by {@link RowLevelOperationInfo#updatedColumns()} plus any columns needed for row
   * lookup or routing, e.g. a primary key).
   * <p>
   * If any of the columns from {@link RowLevelOperationInfo#updatedColumns()}) is
   * missing, an analysis exception is thrown.
   * <p>
   * For updates on nested fields such as {@code SET t.s.c1 = -1} the connector should declare the
   * root struct column {@code s} rather than any nested field.
   */
  NamedReference[] requiredDataAttributes();
}
