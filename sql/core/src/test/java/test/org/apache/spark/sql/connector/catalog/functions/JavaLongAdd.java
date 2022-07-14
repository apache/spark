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

package test.org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaLongAdd implements UnboundFunction {
  private final ScalarFunction<Long> impl;

  public JavaLongAdd(ScalarFunction<Long> impl) {
    this.impl = impl;
  }

  @Override
  public String name() {
    return "long_add";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 2) {
      throw new UnsupportedOperationException("Expect two arguments");
    }
    for (StructField field : inputType.fields()) {
      checkInputType(field.dataType());
    }
    return impl;
  }

  // also allow integer and interval type for testing purpose
  private static void checkInputType(DataType type) {
    if (!(type instanceof IntegerType || type instanceof LongType ||
        type instanceof DayTimeIntervalType)) {
      throw new UnsupportedOperationException(
          "Expect one of [IntegerType|LongType|DateTimeIntervalType] but found " + type);
    }
  }

  @Override
  public String description() {
    return "long_add";
  }

  public abstract static class JavaLongAddBase implements ScalarFunction<Long> {
    private final boolean isResultNullable;

    JavaLongAddBase(boolean isResultNullable) {
      this.isResultNullable = isResultNullable;
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] { DataTypes.LongType, DataTypes.LongType };
    }

    @Override
    public DataType resultType() {
      return DataTypes.LongType;
    }

    @Override
    public boolean isResultNullable() {
      return isResultNullable;
    }
  }

  public static class JavaLongAddDefault extends JavaLongAddBase {
    public JavaLongAddDefault(boolean isResultNullable) {
      super(isResultNullable);
    }

    @Override
    public String name() {
      return "long_add_default";
    }

    @Override
    public Long produceResult(InternalRow input) {
      return input.getLong(0) + input.getLong(1);
    }
  }

  public static class JavaLongAddMagic extends JavaLongAddBase {
    public JavaLongAddMagic(boolean isResultNullable) {
      super(isResultNullable);
    }

    @Override
    public String name() {
      return "long_add_magic";
    }

    public long invoke(long left, long right) {
      return left + right;
    }
  }

  public static class JavaLongAddStaticMagic extends JavaLongAddBase {
    public JavaLongAddStaticMagic(boolean isResultNullable) {
      super(isResultNullable);
    }

    @Override
    public String name() {
      return "long_add_static_magic";
    }

    public static long invoke(long left, long right) {
      return left + right;
    }
  }

  // invalid UDF where type parameters from magic method don't match those from `inputTypes()`
  public static class JavaLongAddMismatchMagic extends JavaLongAddBase {
    public JavaLongAddMismatchMagic() {
      super(false);
    }

    @Override
    public String name() {
      return "long_add_mismatch_magic";
    }

    public long invoke(int left, int right) {
      return left + right;
    }
  }
}
