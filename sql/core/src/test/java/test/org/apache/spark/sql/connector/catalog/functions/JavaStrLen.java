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
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class JavaStrLen implements UnboundFunction {
  private final BoundFunction fn;

  public JavaStrLen(BoundFunction fn) {
    this.fn = fn;
  }

  @Override
  public String name() {
    return "strlen";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 1) {
      throw new UnsupportedOperationException("Expect exactly one argument");
    }

    if (inputType.fields()[0].dataType() instanceof StringType) {
      return fn;
    }

    throw new UnsupportedOperationException("Expect StringType");
  }

  @Override
  public String description() {
    return "strlen: returns the length of the input string\n" +
        " strlen(string) -> int";
  }

  private abstract static class JavaStrLenBase implements ScalarFunction<Integer> {
    @Override
    public DataType[] inputTypes() {
      return new DataType[] { DataTypes.StringType };
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }

    @Override
    public String name() {
      return "strlen";
    }
  }

  public static class JavaStrLenDefault extends JavaStrLenBase {
    @Override
    public Integer produceResult(InternalRow input) {
      String str = input.getString(0);
      return str.length();
    }
  }

  public static class JavaStrLenMagic extends JavaStrLenBase {
    public int invoke(UTF8String str) {
      return str.toString().length();
    }
  }

  public static class JavaStrLenStaticMagic extends JavaStrLenBase {
    public static int invoke(UTF8String str) {
      return str.toString().length();
    }
  }

  public static class JavaStrLenBoth extends JavaStrLenBase {
    @Override
    public Integer produceResult(InternalRow input) {
      String str = input.getString(0);
      return str.length();
    }
    public int invoke(UTF8String str) {
      return str.toString().length() + 100;
    }
  }

  // even though the static magic method is present, it has incorrect parameter type and so Spark
  // should fallback to the non-static magic method
  public static class JavaStrLenBadStaticMagic extends JavaStrLenBase {
    public static int invoke(String str) {
      return str.length();
    }

    public int invoke(UTF8String str) {
      return str.toString().length() + 100;
    }
  }

  public static class JavaStrLenNoImpl extends JavaStrLenBase {
  }

  // a null-safe version which returns 0 for null arguments
  public static class JavaStrLenMagicNullSafe extends JavaStrLenBase {
    public int invoke(UTF8String str) {
      if (str == null) {
        return 0;
      }
      return str.toString().length();
    }
  }

  public static class JavaStrLenStaticMagicNullSafe extends JavaStrLenBase {
    public static int invoke(UTF8String str) {
      if (str == null) {
        return 0;
      }
      return str.toString().length();
    }
  }
}

