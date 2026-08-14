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
   * <p>
   * Two functions that partition data DIFFERENTLY must NOT return the same name. They are
   * indistinguishable to Spark, which can then treat unrelated data as co-partitioned and produce
   * a join with wrong results, so include whatever tells them apart -- argument types, for
   * instance -- in the name. This is the only requirement here that guards results rather than
   * performance.
   * <p>
   * An overriding implementation should keep the name stable across calls, and across separate
   * {@code bind} calls for the same function: Spark binds a function afresh every time it converts
   * a partitioning or ordering reported by a source, so when the two instances are not the same
   * object it has only this name to relate them by when deciding whether two sides of a join are
   * co-partitioned, and so whether a storage-partitioned join can avoid a shuffle. An unstable name
   * is not unsafe -- it just makes overriding pointless, since it relates nothing to anything.
   * <p>
   * This name answers only that question -- whether two transforms are the same PARTITION FUNCTION,
   * ignoring their arguments, which a join needs because it compares {@code bucket(4, left.id)}
   * against {@code bucket(4, right.id)}. It is deliberately not a complete identity for the bound
   * function: it says nothing about {@link #inputTypes()}, {@link #resultType()},
   * {@link #isResultNullable()}, {@link #isDeterministic()}, a {@link ReducibleFunction}'s
   * reducers, or any other state the implementation carries. For the separate question of whether
   * two partition transform expressions are the same expression, see {@link #equals(Object)}.
   * <p>
   * The two questions are related in one direction: this name is the COARSER of the two, so two
   * functions that compare equal must return the same canonical name, while the same name does not
   * make them equal. An implementation that overrides {@link #equals(Object)} should therefore
   * override this method as well -- keeping the default while claiming two instances are equal says
   * they are the same expression but not the same partition function, which costs it exactly the
   * co-partitioning the name exists to enable.
   * <p>
   * Leaving the default in place is allowed: it opts the function out of being recognized as
   * equivalent to anything, which costs optimizations and nothing else. Two instances are never
   * equivalent, so Spark forgoes co-partitioning between them; and because the default is not even
   * stable across two calls on ONE instance, such a function is not the same partition function as
   * itself, so a partitioning it takes part in can cost a shuffle that a stable name would have
   * avoided. Overriding it is the way out of all of that.
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

  /**
   * Implementations SHOULD override {@link Object#equals(Object)} and {@link Object#hashCode()}.
   * <p>
   * Spark binds a function afresh every time it converts a partitioning or ordering reported by a
   * source, so two reports of the same transform -- from two scans of the same table, say -- hold
   * two bound instances. Spark compares the partition transform expressions built from them with
   * ordinary expression equality, which reaches this method. With the inherited identity comparison
   * the two are never equal, and Spark cannot deduplicate the expressions, reuse a scan that
   * reports them, or recognize two subplans as the same. That costs optimizations only: results
   * are unaffected either way.
   * <p>
   * Compare whatever state affects behaviour. {@link #canonicalName()} on its own is not
   * necessarily enough: it answers a narrower question and is allowed to be coarse, so two
   * functions can share one and still differ in {@link #inputTypes()}, {@link #resultType()},
   * {@link #isResultNullable()}, {@link #isDeterministic()}, or a {@link ReducibleFunction}'s
   * reducers. Comparing the name IS sufficient for an implementation whose names already encode
   * everything that distinguishes its functions -- one name per argument type, say. Only the
   * implementation knows which of its state matters, which is why Spark cannot derive this
   * comparison itself. Note that a coarse name does not have to carry the whole identity: Spark
   * compares the transform's arguments separately.
   * <p>
   * This is the FINER of the two comparisons, so two functions that compare equal must also return
   * the same {@link #canonicalName()}; an implementation overriding this method should override
   * that one too, rather than leave it at its default.
   * <p>
   * Two functions that can produce DIFFERENT values must NOT compare equal. Being too coarse is the
   * direction that costs more than optimizations, and it is not confined to partition transforms: a
   * {@link ScalarFunction} or an {@link AggregateFunction} is held by the expression Spark builds
   * for an ordinary call to it, so the same {@code equals} decides whether two such calls are the
   * same expression, and Spark may then evaluate one where the query asked for the other.
   * <p>
   * Whatever is compared must be stable across {@code bind} calls, and {@code hashCode} must agree
   * with {@code equals}, since Spark puts these expressions in hash-based collections.
   */
  @Override
  boolean equals(Object other);

  /**
   * Implementations SHOULD override this together with {@link #equals(Object)}, and it must agree
   * with it: Spark puts the expressions holding this function in hash-based collections, so an
   * {@code equals} without a matching {@code hashCode} buys nothing. See {@link #equals(Object)}.
   */
  @Override
  int hashCode();
}
