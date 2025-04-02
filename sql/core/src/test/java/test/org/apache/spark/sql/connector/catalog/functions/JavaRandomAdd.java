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

import java.util.Random;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;

/**
 * Test V2 function which add a random number to the input integer.
 */
public class JavaRandomAdd implements UnboundFunction {
  private final BoundFunction fn;

  public JavaRandomAdd(BoundFunction fn) {
    this.fn = fn;
  }

  @Override
  public String name() {
    return "rand";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 1) {
      throw new UnsupportedOperationException("Expect exactly one argument");
    }
    if (inputType.fields()[0].dataType() instanceof IntegerType) {
      return fn;
    }
    throw new UnsupportedOperationException("Expect IntegerType");
  }

  @Override
  public String description() {
    return "rand_add: add a random integer to the input\n" +
      "rand_add(int) -> int";
  }

  public abstract static class JavaRandomAddBase implements ScalarFunction<Integer> {
    @Override
    public DataType[] inputTypes() {
      return new DataType[] { DataTypes.IntegerType };
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }

    @Override
    public String name() {
      return "rand_add";
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class JavaRandomAddDefault extends JavaRandomAddBase {
    private final Random rand = new Random();

    @Override
    public Integer produceResult(InternalRow input) {
      return input.getInt(0) + rand.nextInt();
    }
  }

  public static class JavaRandomAddMagic extends JavaRandomAddBase {
    private final Random rand = new Random();

    public int invoke(int input) {
      return input + rand.nextInt();
    }
  }

  public static class JavaRandomAddStaticMagic extends JavaRandomAddBase {
    private static final Random rand = new Random();

    public static int invoke(int input) {
      return input + rand.nextInt();
    }
  }
}

