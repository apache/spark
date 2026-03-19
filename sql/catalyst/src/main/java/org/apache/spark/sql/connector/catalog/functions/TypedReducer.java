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
import org.apache.spark.sql.types.DataType;

/**
 * A {@link Reducer} that changes the {@link DataType data type} of the values it produces.
 *
 * <p>Implement this interface instead of {@link Reducer} when the reducer produces values of a
 * different logical Spark {@link DataType data type} than its input. If a {@link ReducibleFunction}
 * returns a {@code TypedReducer} from its {@code reducer()} method, the output type is taken from
 * {@link #resultType()}. If it returns a plain {@link Reducer}, the output type is assumed to be
 * unchanged.
 *
 * @param <I> the physical Java type of the input
 * @param <O> the physical Java type of the output
 * @see Reducer
 * @see ReducibleFunction
 * @since 4.2.0
 */
@Evolving
public interface TypedReducer<I, O> extends Reducer<I, O> {
  /**
   * Returns the logical Spark {@link DataType data type} of values produced by this reducer.
   *
   * @return the data type of values produced by this reducer.
   */
  DataType resultType();
}
