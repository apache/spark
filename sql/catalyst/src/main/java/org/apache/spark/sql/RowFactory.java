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

package org.apache.spark.sql;

import org.apache.spark.annotation.Stable;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

/**
 * A factory class used to construct {@link Row} objects.
 *
 * @since 1.3.0
 */
@Stable
public class RowFactory {

  /**
   * Create a {@link Row} from the given arguments. Position i in the argument list becomes
   * position i in the created {@link Row} object.
   *
   * @since 1.3.0
   */
  public static Row create(Object ... values) {
    return new GenericRow(values);
  }

  /**
   * Create a {@link Row} from the given arguments. Provided schema is incorporated into
   * created {@link Row} object, and allows getAs(fieldName) to access the value of column.
   *
   * Note that every Rows will contain the duplicated schema, hence in high volume it is still
   * recommended to use `create` with accessing column by position.
   *
   * @since 3.0.0
   */
  public static Row createWithSchema(StructType schema, Object ... values) {
    return new GenericRowWithSchema(values, schema);
  }
}
