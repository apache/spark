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

/**
 * A StructField object represents a field in a StructType object.
 * A StructField object comprises three fields, {@code String name}, {@code DataType dataType},
 * and {@code boolean nullable}. The field of {@code name} is the name of a StructField.
 * The field of {@code dataType} specifies the data type of a StructField.
 * The field of {@code nullable} specifies if values of a StructField can contain {@code null}
 * values.
 *
 * To create a {@link StructField},
 * {@link org.apache.spark.sql.api.java.types.DataType#createStructField(String, DataType, boolean)}
 * should be used.
 */
public class StructField {
  private String name;
  private DataType dataType;
  private boolean nullable;

  protected StructField(String name, DataType dataType, boolean nullable) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
  }

  public String getName() {
    return name;
  }

  public DataType getDataType() {
    return dataType;
  }

  public boolean isNullable() {
    return nullable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StructField that = (StructField) o;

    if (nullable != that.nullable) return false;
    if (!dataType.equals(that.dataType)) return false;
    if (!name.equals(that.name)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + dataType.hashCode();
    result = 31 * result + (nullable ? 1 : 0);
    return result;
  }
}
