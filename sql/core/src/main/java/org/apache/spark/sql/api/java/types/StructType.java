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

package org.apache.spark.sql.api.java.types;

import java.util.Arrays;
import java.util.List;

/**
 * The data type representing Rows.
 * A StructType object comprises an array of StructFields.
 *
 * To create an {@link StructType},
 * {@link org.apache.spark.sql.api.java.types.DataType#createStructType(java.util.List)} or
 * {@link org.apache.spark.sql.api.java.types.DataType#createStructType(StructField[])}
 * should be used.
 */
public class StructType extends DataType {
  private StructField[] fields;

  protected StructType(StructField[] fields) {
    this.fields = fields;
  }

  public StructField[] getFields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StructType that = (StructType) o;

    if (!Arrays.equals(fields, that.fields)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(fields);
  }
}
