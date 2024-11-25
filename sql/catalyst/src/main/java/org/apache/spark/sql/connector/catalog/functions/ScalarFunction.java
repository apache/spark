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

package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.QuotingUtils;
import org.apache.spark.sql.types.DataType;

import java.util.Map;

/**
 * Interface for a function that produces a result value for each input row.
 * <p>
 * To evaluate each input row, Spark will first try to lookup and use a "magic method" (described
 * below) through Java reflection. If the method is not found, Spark will call
 * {@link #produceResult(InternalRow)} as a fallback approach.
 * <p>
 * The JVM type of result values produced by this function must be the type used by Spark's
 * InternalRow API for the {@link DataType SQL data type} returned by {@link #resultType()}.
 * The mapping between {@link DataType} and the corresponding JVM type is defined below.
 * <p>
 * <h2> Magic method </h2>
 * <b>IMPORTANT</b>: the default implementation of {@link #produceResult} throws
 * {@link UnsupportedOperationException}. Users must choose to either override this method, or
 * implement a magic method with name {@link #MAGIC_METHOD_NAME}, which takes individual parameters
 * instead of a {@link InternalRow}. The magic method approach is generally recommended because it
 * provides better performance over the default {@link #produceResult}, due to optimizations such
 * as whole-stage codegen, elimination of Java boxing, etc.
 * <p>
 * The type parameters for the magic method <b>must match</b> those returned from
 * {@link BoundFunction#inputTypes()}. Otherwise Spark will not be able to find the magic method.
 * <p>
 * In addition, for stateless Java functions, users can optionally define the
 * {@link #MAGIC_METHOD_NAME} as a static method, which further avoids certain runtime costs such
 * as Java dynamic dispatch.
 * <p>
 * For example, a scalar UDF for adding two integers can be defined as follow with the magic
 * method approach:
 *
 * <pre>
 *   public class IntegerAdd implements{@code ScalarFunction<Integer>} {
 *     public DataType[] inputTypes() {
 *       return new DataType[] { DataTypes.IntegerType, DataTypes.IntegerType };
 *     }
 *     public int invoke(int left, int right) {
 *       return left + right;
 *     }
 *   }
 * </pre>
 * In the above, since {@link #MAGIC_METHOD_NAME} is defined, and also that it has
 * matching parameter types and return type, Spark will use it to evaluate inputs.
 * <p>
 * As another example, in the following:
 * <pre>
 *   public class IntegerAdd implements{@code ScalarFunction<Integer>} {
 *     public DataType[] inputTypes() {
 *       return new DataType[] { DataTypes.IntegerType, DataTypes.IntegerType };
 *     }
 *     public static int invoke(int left, int right) {
 *       return left + right;
 *     }
 *     public Integer produceResult(InternalRow input) {
 *       return input.getInt(0) + input.getInt(1);
 *     }
 *   }
 * </pre>
 *
 * the class defines both the magic method and the {@link #produceResult}, and Spark will use
 * {@link #MAGIC_METHOD_NAME} over the {@link #produceResult(InternalRow)} as it takes higher
 * precedence. Also note that the magic method is annotated as a static method in this case.
 * <p>
 * Resolution on magic method is done during query analysis, where Spark looks up the magic
 * method by first converting the actual input SQL data types to their corresponding Java types
 * following the mapping defined below, and then checking if there is a matching method from all the
 * declared methods in the UDF class, using method name and the Java types.
 * <p>
 * <h2> Handling of nullable primitive arguments </h2>
 * The handling of null primitive arguments is different between the magic method approach and
 * the {@link #produceResult} approach. With the former, whenever any of the method arguments meet
 * the following conditions:
 * <ol>
 *   <li>the argument is of primitive type</li>
 *   <li>the argument is nullable</li>
 *   <li>the value of the argument is null</li>
 * </ol>
 * Spark will return null directly instead of calling the magic method. On the other hand, Spark
 * will pass null primitive arguments to {@link #produceResult} and it is user's responsibility to
 * handle them in the function implementation.
 * <p>
 * Because of the difference, if Spark users want to implement special handling of nulls for
 * nullable primitive arguments, they should override the {@link #produceResult} method instead
 * of using the magic method approach.
 * <p>
 * <h2> Spark data type to Java type mapping </h2>
 * The following are the mapping from {@link DataType SQL data type} to Java type which is used
 * by Spark to infer parameter types for the magic methods as well as return value type for
 * {@link #produceResult}:
 * <ul>
 *   <li>{@link org.apache.spark.sql.types.BooleanType}: {@code boolean}</li>
 *   <li>{@link org.apache.spark.sql.types.ByteType}: {@code byte}</li>
 *   <li>{@link org.apache.spark.sql.types.ShortType}: {@code short}</li>
 *   <li>{@link org.apache.spark.sql.types.IntegerType}: {@code int}</li>
 *   <li>{@link org.apache.spark.sql.types.LongType}: {@code long}</li>
 *   <li>{@link org.apache.spark.sql.types.FloatType}: {@code float}</li>
 *   <li>{@link org.apache.spark.sql.types.DoubleType}: {@code double}</li>
 *   <li>{@link org.apache.spark.sql.types.StringType}:
 *       {@link org.apache.spark.unsafe.types.UTF8String}</li>
 *   <li>{@link org.apache.spark.sql.types.DateType}: {@code int}</li>
 *   <li>{@link org.apache.spark.sql.types.TimestampType}: {@code long}</li>
 *   <li>{@link org.apache.spark.sql.types.BinaryType}: {@code byte[]}</li>
 *   <li>{@link org.apache.spark.sql.types.DayTimeIntervalType}: {@code long}</li>
 *   <li>{@link org.apache.spark.sql.types.YearMonthIntervalType}: {@code int}</li>
 *   <li>{@link org.apache.spark.sql.types.DecimalType}:
 *       {@link org.apache.spark.sql.types.Decimal}</li>
 *   <li>{@link org.apache.spark.sql.types.StructType}: {@link InternalRow}</li>
 *   <li>{@link org.apache.spark.sql.types.ArrayType}:
 *       {@link org.apache.spark.sql.catalyst.util.ArrayData}</li>
 *   <li>{@link org.apache.spark.sql.types.MapType}:
 *       {@link org.apache.spark.sql.catalyst.util.MapData}</li>
 * </ul>
 *
 * @param <R> the JVM type of result values, MUST be consistent with the {@link DataType}
 *          returned via {@link #resultType()}, according to the mapping above.
 *
 * @since 3.2.0
 */
@Evolving
public interface ScalarFunction<R> extends BoundFunction {
  String MAGIC_METHOD_NAME = "invoke";

  /**
   * Applies the function to an input row to produce a value.
   *
   * @param input an input row
   * @return a result value
   */
  default R produceResult(InternalRow input) {
    throw new SparkUnsupportedOperationException(
      "SCALAR_FUNCTION_NOT_COMPATIBLE",
      Map.of("scalarFunc", QuotingUtils.quoteIdentifier(name()))
    );
  }

}
