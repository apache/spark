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
 * @since 4.0.0
 */
@Evolving
public interface ReducibleFunction<T, A> extends ScalarFunction<T> {

    /**
     * If this function is 'reducible' on another function, return the {@link Reducer} function.
     * @param other other function
     * @param thisArgument argument for this function instance
     * @param otherArgument argument for other function instance
     * @return a reduction function if it is reducible, none if not
     */
    Option<Reducer<A>> reducer(ReducibleFunction<?, ?> other, Option<?> thisArgument, Option<?> otherArgument);
}
