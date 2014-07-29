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
 * The data type representing Lists.
 * An ArrayType object comprises two fields, {@code DataType elementType} and
 * {@code boolean containsNull}. The field of {@code elementType} is used to specify the type of
 * array elements. The field of {@code containsNull} is used to specify if the array has
 * {@code null} values.
 *
 * To create an {@link ArrayType},
 * {@link org.apache.spark.sql.api.java.types.DataType#createArrayType(DataType)} or
 * {@link org.apache.spark.sql.api.java.types.DataType#createArrayType(DataType, boolean)}
 * should be used.
 */
public class ArrayType extends DataType {
  private DataType elementType;
  private boolean containsNull;

  protected ArrayType(DataType elementType, boolean containsNull) {
    this.elementType = elementType;
    this.containsNull = containsNull;
  }

  public DataType getElementType() {
    return elementType;
  }

  public boolean isContainsNull() {
    return containsNull;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ArrayType arrayType = (ArrayType) o;

    if (containsNull != arrayType.containsNull) return false;
    if (!elementType.equals(arrayType.elementType)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = elementType.hashCode();
    result = 31 * result + (containsNull ? 1 : 0);
    return result;
  }
}
