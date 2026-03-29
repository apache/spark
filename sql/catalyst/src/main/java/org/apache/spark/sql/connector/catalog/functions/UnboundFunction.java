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
import org.apache.spark.sql.types.StructType;

/**
 * Represents a user-defined function that is not bound to input types.
 *
 * @since 3.2.0
 */
@Evolving
public interface UnboundFunction extends Function {

  /**
   * Bind this function to an input type.
   * <p>
   * If the input type is not supported, implementations must throw
   * {@link UnsupportedOperationException}.
   * <p>
   * For example, a "length" function that only supports a single string argument should throw
   * UnsupportedOperationException if the struct has more than one field or if that field is not a
   * string, and it may optionally throw if the field is nullable.
   *
   * @param inputType a struct type for inputs that will be passed to the bound function
   * @return a function that can process rows with the given input type
   * @throws UnsupportedOperationException If the function cannot be applied to the input type
   */
  BoundFunction bind(StructType inputType);

  /**
   * Returns Function documentation.
   *
   * @return this function's documentation
   */
  String description();

}
