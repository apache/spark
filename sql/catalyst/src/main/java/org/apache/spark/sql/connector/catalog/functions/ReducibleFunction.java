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
import scala.Option;

/**
 * Base class for user-defined functions that can be 'reduced' on another function.
 *
 * A function f_source(x) is 'reducible' on another function f_target(x) if
 * there exists a reducer function r(x) such that r(f_source(x)) = f_target(x) for all input x.
 *
 * <p>
 * Examples:
 * <ul>
 *    <li>Bucket functions
 *    <ul>
 *        <li>f_source(x) = bucket(4, x)</li>
 *        <li>f_target(x) = bucket(2, x)</li>
 *        <li>r(x) = x / 2</li>
 *     </ul>
 *    <li>Date functions</li>
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
     * If this function is 'reducible' on another function, return the {@link Reducer} function.
     * <p>
     * Example:
     * <ul>
     *     <li>this_function = bucket(4, x)
     *     <li>other function = bucket(2, x)
     * </ul>
     * Invoke with arguments
     * <ul>
     *     <li>other = bucket</li>
     *     <li>this param = Int(4)</li>
     *     <li>other param = Int(2)</li>
     * </ul>
     * @param other the other function
     * @param thisParam param for this function
     * @param otherParam param for the other function
     * @return a reduction function if it is reducible, none if not
     */
    Option<Reducer<I, O>> reducer(ReducibleFunction<?, ?> other, Option<?> thisParam,
                               Option<?> otherParam);
}
