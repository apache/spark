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
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;

import java.util.UUID;

/**
 * Represents a function that is bound to an input type.
 *
 * @since 3.2.0
 */
@Evolving
public interface BoundFunction extends Function {

  /**
   * Returns the required {@link DataType data types} of the input values to this function.
   * <p>
   * If the types returned differ from the types passed to {@link UnboundFunction#bind(StructType)},
   * Spark will cast input values to the required data types. This allows implementations to
   * delegate input value casting to Spark.
   *
   * @return an array of input value data types
   */
  DataType[] inputTypes();

  /**
   * Returns the {@link DataType data type} of values produced by this function.
   * <p>
   * For example, a "plus" function may return {@link IntegerType} when it is bound to arguments
   * that are also {@link IntegerType}.
   *
   * @return a data type for values produced by this function
   */
  DataType resultType();

  /**
   * Returns whether the values produced by this function may be null.
   * <p>
   * For example, a "plus" function may return false when it is bound to arguments that are always
   * non-null, but true when either argument may be null.
   *
   * @return true if values produced by this function may be null, false otherwise
   */
  default boolean isResultNullable() {
    return true;
  }

  /**
   * Returns whether this function result is deterministic.
   * <p>
   * By default, functions are assumed to be deterministic. Functions that are not deterministic
   * should override this method so that Spark can ensure the function runs only once for a given
   * input.
   *
   * @return true if this function is deterministic, false otherwise
   */
  default boolean isDeterministic() {
    return true;
  }

  /**
   * Returns the canonical name of this function, used to determine if functions are equivalent.
   * <p>
   * The canonical name is used to determine whether two functions are the same when loaded by
   * different catalogs. For example, the same catalog implementation may be used for by two
   * environments, "prod" and "test". Functions produced by the catalogs may be equivalent, but
   * loaded using different names, like "test.func_name" and "prod.func_name".
   * <p>
   * Names returned by this function should be unique and unlikely to conflict with similar
   * functions in other catalogs. For example, many catalogs may define a "bucket" function with a
   * different implementation. Adding context, like "com.mycompany.bucket(string)", is recommended
   * to avoid unintentional collisions.
   *
   * @return a canonical name for this function
   */
  default String canonicalName() {
    // by default, use a random UUID so a function is never equivalent to another, even itself.
    // this method is not required so that generated implementations (or careless ones) are not
    // added and forgotten. for example, returning "" as a place-holder could cause unnecessary
    // bugs if not replaced before release.
    return UUID.randomUUID().toString();
  }
}
