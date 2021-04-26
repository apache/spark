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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

/**
 * Interface for a function that produces a result value for each input row.
 * <p>
 * To evaluate each input row, Spark will first try to lookup and use a "magic method" (described
 * below) through Java reflection. If the method is not found, Spark will call
 * {@link #produceResult(InternalRow)} as a fallback approach.
 * <p>
 * The JVM type of result values produced by this function must be the type used by Spark's
 * InternalRow API for the {@link DataType SQL data type} returned by {@link #resultType()}.
 * <p>
 * <b>IMPORTANT</b>: the default implementation of {@link #produceResult} throws
 * {@link UnsupportedOperationException}. Users can choose to override this method, or implement
 * a "magic method" with name {@link #MAGIC_METHOD_NAME} which takes individual parameters
 * instead of a {@link InternalRow}. The magic method will be loaded by Spark through Java
 * reflection and will also provide better performance in general, due to optimizations such as
 * codegen, removal of Java boxing, etc.
 *
 * For example, a scalar UDF for adding two integers can be defined as follow with the magic
 * method approach:
 *
 * <pre>
 *   public class IntegerAdd implements{@code ScalarFunction<Integer>} {
 *     public int invoke(int left, int right) {
 *       return left + right;
 *     }
 *   }
 * </pre>
 * In this case, since {@link #MAGIC_METHOD_NAME} is defined, Spark will use it over
 * {@link #produceResult} to evalaute the inputs. In general Spark looks up the magic method by
 * first converting the actual input SQL data types to their corresponding Java types following
 * the mapping defined below, and then checking if there is a matching method from all the
 * declared methods in the UDF class, using method name (i.e., {@link #MAGIC_METHOD_NAME}) and
 * the Java types. If no magic method is found, Spark will falls back to use {@link #produceResult}.
 * <p>
 * The following are the mapping from {@link DataType SQL data type} to Java type through
 * the magic method approach:
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
 * @param <R> the JVM type of result values
 */
public interface ScalarFunction<R> extends BoundFunction {
  String MAGIC_METHOD_NAME = "invoke";

  /**
   * Applies the function to an input row to produce a value.
   *
   * @param input an input row
   * @return a result value
   */
  default R produceResult(InternalRow input) {
    throw new UnsupportedOperationException(
        "Cannot find a compatible ScalarFunction#produceResult");
  }

}
