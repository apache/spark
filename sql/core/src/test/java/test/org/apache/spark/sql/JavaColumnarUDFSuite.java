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

package test.org.apache.spark.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.columnar.ColumnarScalarFunction;
import org.apache.spark.sql.columnar.ColumnarUDFImplementation;
import org.apache.spark.sql.columnar.ColumnarUserDefinedFunction;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import static org.apache.spark.sql.functions.col;

public class JavaColumnarUDFSuite {
  private transient SparkSession spark;

  /** Adds one to a bigint, and keeps nulls. */
  public static class AddOne implements ColumnarScalarFunction {
    @Override
    public ColumnVector eval(ColumnarBatch args) {
      int numRows = args.numRows();
      ColumnVector input = args.column(0);
      OnHeapColumnVector result = new OnHeapColumnVector(numRows, DataTypes.LongType);
      for (int i = 0; i < numRows; i++) {
        if (input.isNullAt(i)) {
          result.putNull(i);
        } else {
          result.putLong(i, input.getLong(i) + 1);
        }
      }
      return result;
    }
  }

  @BeforeEach
  public void setUp() {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void columnarUDF() {
    ColumnarUserDefinedFunction addOne = ColumnarUserDefinedFunction.create(
      "java_add_one", new DataType[] { DataTypes.LongType }, DataTypes.LongType, new AddOne());
    Assertions.assertEquals("java_add_one", addOne.name());
    Assertions.assertEquals(DataTypes.LongType, addOne.dataType());
    Assertions.assertTrue(addOne.deterministic());
    Assertions.assertTrue(addOne.nullable());

    // A later change of the array does not change the function.
    DataType[] types = { DataTypes.LongType };
    ColumnarUserDefinedFunction copied =
      ColumnarUserDefinedFunction.create("copied", types, DataTypes.LongType, new AddOne());
    types[0] = DataTypes.StringType;
    Assertions.assertEquals(1, copied.inputTypes().length());
    Assertions.assertEquals(DataTypes.LongType, copied.inputTypes().apply(0));

    List<Row> applied = spark.range(3).select(addOne.apply(col("id"))).collectAsList();
    Assertions.assertEquals(3, applied.size());
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(i + 1L, applied.get(i).getLong(0));
    }

    spark.udf().registerColumnar(addOne);
    List<Row> called = spark.sql("SELECT java_add_one(id) FROM range(3)").collectAsList();
    Assertions.assertEquals(applied, called);
  }

  @Test
  public void columnarUDFWithEngineImplementation() {
    Map<String, String> properties = new HashMap<>();
    properties.put("library", "/lib.so");
    ColumnarUDFImplementation implementation =
      ColumnarUDFImplementation.create("some-engine", properties);
    // A later change of the map does not change the implementation.
    properties.put("name", "f");
    Assertions.assertEquals("some-engine", implementation.kind());
    Assertions.assertEquals(1, implementation.properties().size());
    Assertions.assertEquals("/lib.so", implementation.properties().get("library"));
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> implementation.properties().put("k", "v"));

    ColumnarUserDefinedFunction engineOnly = ColumnarUserDefinedFunction
      .create("java_engine_only", new DataType[] { DataTypes.LongType }, DataTypes.LongType)
      .withEngineImplementation(implementation)
      .asNondeterministic()
      .asNonNullable();
    Assertions.assertFalse(engineOnly.deterministic());
    Assertions.assertFalse(engineOnly.nullable());

    // Spark cannot run the function, and the error names the kinds of its implementations.
    SparkUnsupportedOperationException e = Assertions.assertThrows(
      SparkUnsupportedOperationException.class,
      () -> spark.range(1).select(engineOnly.apply(col("id"))).collectAsList());
    Assertions.assertEquals("COLUMNAR_UDF_NOT_EXECUTABLE", e.getCondition());
    Assertions.assertEquals("[some-engine]", e.getMessageParameters().get("kinds"));
  }
}
