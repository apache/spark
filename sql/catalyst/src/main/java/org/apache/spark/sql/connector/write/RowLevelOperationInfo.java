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
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * An interface with logical information for a row-level operation such as DELETE, UPDATE, MERGE.
 *
 * @since 3.3.0
 */
@Experimental
public interface RowLevelOperationInfo {
  /**
   * Returns options that the user specified when performing the row-level operation.
   */
  CaseInsensitiveStringMap options();

  /**
   * Returns the row-level SQL command (e.g. DELETE, UPDATE, MERGE).
   */
  Command command();

  /**
   * Returns the columns being updated by this operation. Currently only UPDATE populates it;
   * other commands report an empty array.
   * <p>
   * A column is reported unless its assigned value is the column itself, so identity assignments
   * such as {@code SET a = a} are excluded. With the default CHAR and VARCHAR handling, a
   * self-assignment on a CHAR or VARCHAR column is still reported, because Spark pads or
   * length-checks the assigned value. Nested struct field updates are reported at root-column
   * granularity (e.g. {@code SET s.c1 = -1} returns {@code s}), even when the field is assigned to
   * itself.
   *
   * @since 4.4.0
   */
  NamedReference[] updatedColumns();
}
