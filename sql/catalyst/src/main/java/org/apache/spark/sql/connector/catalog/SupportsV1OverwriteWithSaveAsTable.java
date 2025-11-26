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
 * A marker interface that can be mixed into a {@link Table} to indicate that the data
 * source needs to distinguish between DataFrameWriter V1 {@code saveAsTable} operations and
 * DataFrameWriter V2 {@code createOrReplace}/{@code replace} operations during overwrite/replace.
 * <p>
 * Background: DataFrameWriter V1's {@code saveAsTable} with {@code SaveMode.Overwrite} creates
 * a {@code ReplaceTableAsSelect} logical plan if the table is V2, which is identical to the plan
 * created by DataFrameWriter V2's {@code createOrReplace}. However, the documented semantics can
 * have different interpretations:
 * <ul>
 *   <li>V1 saveAsTable with Overwrite: "if data/table already exists, existing data is expected
 *       to be overwritten by the contents of the DataFrame" - does not define behavior for
 *       metadata (schema) overwriting</li>
 *   <li>V2 createOrReplace: "The output table's schema, partition layout, properties, and other
 *       configuration will be based on the contents of the data frame... If the table exists,
 *       its configuration and data will be replaced"</li>
 * </ul>
 * <p>
 * Data sources that migrated from V1 to V2 may have adopted different behaviors based on these
 * documented semantics. For example, Delta Lake interprets V1 saveAsTable to not replace table
 * schema unless the {@code overwriteSchema} option is explicitly set.
 * <p>
 * When a {@link Table} implements this interface and
 * {@link #addV1OverwriteWithSaveAsTableOption()} returns {@code true}, DataFrameWriter V1
 * with mode Overwrite will add an internal write option to indicate that the command originated
 * from V1 saveAsTable API. This allows the data source to distinguish between the two APIs and
 * apply appropriate semantics.
 * <p>
 * The option key used is defined by {@link #OPTION_NAME} and the value will be "true" when
 * the write originates from DataFrameWriter V1 saveAsTable with Overwrite mode.
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsV1OverwriteWithSaveAsTable extends Table {
  /**
   * The name of the internal write option that indicates the command originated from
   * DataFrameWriter V1 saveAsTable API with Overwrite mode.
   */
  String OPTION_NAME = "__v1_save_as_table_overwrite";

  /**
   * Returns whether to add the marker write option for V1 saveAsTable with Overwrite mode.
   * <p>
   * By default, this returns {@code true}. Implementations can override this to dynamically
   * decide whether to add the marker option, for example to gradually migrate away from
   * legacy V1 behavior on a table-by-table basis.
   *
   * @return {@code true} if the marker write option should be added, {@code false} otherwise
   */
  default boolean addV1OverwriteWithSaveAsTableOption() {
    return true;
  }
}
