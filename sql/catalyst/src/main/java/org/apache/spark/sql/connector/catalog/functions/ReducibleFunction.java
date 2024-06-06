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

import org.apache.spark.annotation.Evolving;

/**
 * Base class for user-defined functions that can be 'reduced' on another function.
 *
 * A function f_source(x) is 'reducible' on another function f_target(x) if
 * <ul>
 *   <li> There exists a reducer function r(x) such that r(f_source(x)) = f_target(x)
 *        for all input x, or </li>
 *   <li> More generally, there exists reducer functions r1(x) and r2(x) such that
 *        r1(f_source(x)) = r2(f_target(x)) for all input x. </li>
 * </ul>
 * <p>
 * Examples:
 * <ul>
 *    <li>Bucket functions where one side has reducer
 *    <ul>
 *        <li>f_source(x) = bucket(4, x)</li>
 *        <li>f_target(x) = bucket(2, x)</li>
 *        <li>r(x) = x % 2</li>
 *    </ul>
 *
 *    <li>Bucket functions where both sides have reducer
 *    <ul>
 *        <li>f_source(x) = bucket(16, x)</li>
 *        <li>f_target(x) = bucket(12, x)</li>
 *        <li>r1(x) = x % 4</li>
 *        <li>r2(x) = x % 4</li>
 *    </ul>
 *
 *    <li>Date functions
 *    <ul>
 *        <li>f_source(x) = days(x)</li>
 *        <li>f_target(x) = hours(x)</li>
 *        <li>r(x) = x / 24</li>
 *     </ul>
 * </ul>
 * @param <I> reducer function input type
 * @param <O> reducer function output type
 * @since 4.0.0
 */
@Evolving
public interface ReducibleFunction<I, O> {

  /**
   * This method is for the bucket function.
   *
   * If this bucket function is 'reducible' on another bucket function,
   * return the {@link Reducer} function.
   * <p>
   * For example, to return reducer for reducing f_source = bucket(4, x) on f_target = bucket(2, x)
   * <ul>
   *     <li>thisBucketFunction = bucket</li>
   *     <li>thisNumBuckets = 4</li>
   *     <li>otherBucketFunction = bucket</li>
   *     <li>otherNumBuckets = 2</li>
   * </ul>
   *
   * @param thisNumBuckets parameter for this function
   * @param otherBucketFunction the other parameterized function
   * @param otherNumBuckets parameter for the other function
   * @return a reduction function if it is reducible, null if not
   */
  default Reducer<I, O> reducer(
      int thisNumBuckets,
      ReducibleFunction<?, ?> otherBucketFunction,
      int otherNumBuckets) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method is for all other functions.
   *
   * If this function is 'reducible' on another function, return the {@link Reducer} function.
   * <p>
   * Example of reducing f_source = days(x) on f_target = hours(x)
   * <ul>
   *     <li>thisFunction = days</li>
   *     <li>otherFunction = hours</li>
   * </ul>
   *
   * @param otherFunction the other function
   * @return a reduction function if it is reducible, null if not.
   */
  default Reducer<I, O> reducer(ReducibleFunction<?, ?> otherFunction) {
    throw new UnsupportedOperationException();
  }
}
