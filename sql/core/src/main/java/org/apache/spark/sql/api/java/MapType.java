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

package org.apache.spark.sql.api.java;

/**
 * The data type representing Maps. A MapType object comprises two fields,
 * {@code DataType keyType}, {@code DataType valueType}, and {@code boolean valueContainsNull}.
 * The field of {@code keyType} is used to specify the type of keys in the map.
 * The field of {@code valueType} is used to specify the type of values in the map.
 * The field of {@code valueContainsNull} is used to specify if map values have
 * {@code null} values.
 * For values of a MapType column, keys are not allowed to have {@code null} values.
 *
 * To create a {@link MapType},
 * {@link DataType#createMapType(DataType, DataType)} or
 * {@link DataType#createMapType(DataType, DataType, boolean)}
 * should be used.
 */
public class MapType extends DataType {
  private DataType keyType;
  private DataType valueType;
  private boolean valueContainsNull;

  protected MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.valueContainsNull = valueContainsNull;
  }

  public DataType getKeyType() {
    return keyType;
  }

  public DataType getValueType() {
    return valueType;
  }

  public boolean isValueContainsNull() {
    return valueContainsNull;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MapType mapType = (MapType) o;

    if (valueContainsNull != mapType.valueContainsNull) return false;
    if (!keyType.equals(mapType.keyType)) return false;
    if (!valueType.equals(mapType.valueType)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = keyType.hashCode();
    result = 31 * result + valueType.hashCode();
    result = 31 * result + (valueContainsNull ? 1 : 0);
    return result;
  }
}
