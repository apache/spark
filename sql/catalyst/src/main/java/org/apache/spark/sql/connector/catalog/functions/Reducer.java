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
 * A 'reducer' for output of user-defined functions.
 *
 * A user_defined function f_source(x) is 'reducible' on another user_defined function f_target(x),
 * if there exists a 'reducer' r(x) such that r(f_source(x)) = f_target(x) for all input x.
 * @param <T> function output type
 * @since 4.0.0
 */
@Evolving
public interface Reducer<T> {
    T reduce(T arg1);
}
