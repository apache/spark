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
package org.apache.spark.sql

import java.lang

abstract class TableValuedFunction {

  /**
   * Creates a `Dataset` with a single `LongType` column named `id`, containing elements in a
   * range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 4.0.0
   */
  def range(end: Long): Dataset[lang.Long]

  /**
   * Creates a `Dataset` with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 4.0.0
   */
  def range(start: Long, end: Long): Dataset[lang.Long]

  /**
   * Creates a `Dataset` with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value.
   *
   * @since 4.0.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[lang.Long]

  /**
   * Creates a `Dataset` with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value, with partition number specified.
   *
   * @since 4.0.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[lang.Long]

  /**
   * Creates a `DataFrame` containing a new row for each element in the given array or map column.
   * Uses the default column name `col` for elements in the array and `key` and `value` for
   * elements in the map unless specified otherwise.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def explode(collection: Column): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for each element in the given array or map column.
   * Uses the default column name `col` for elements in the array and `key` and `value` for
   * elements in the map unless specified otherwise. Unlike explode, if the array/map is null or
   * empty then null is produced.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def explode_outer(collection: Column): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for each element in the given array of structs.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def inline(input: Column): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for each element in the given array of structs.
   * Unlike inline, if the array is null or empty then null is produced for each nested column.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def inline_outer(input: Column): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for a json column according to the given field
   * names.
   *
   * @group json_funcs
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def json_tuple(input: Column, fields: Column*): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for each element with position in the given array
   * or map column. Uses the default column name `pos` for position, and `col` for elements in the
   * array and `key` and `value` for elements in the map unless specified otherwise.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def posexplode(collection: Column): Dataset[Row]

  /**
   * Creates a `DataFrame` containing a new row for each element with position in the given array
   * or map column. Uses the default column name `pos` for position, and `col` for elements in the
   * array and `key` and `value` for elements in the map unless specified otherwise. Unlike
   * posexplode, if the array/map is null or empty then the row (null, null) is produced.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def posexplode_outer(collection: Column): Dataset[Row]

  /**
   * Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default
   * unless specified otherwise.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def stack(n: Column, fields: Column*): Dataset[Row]

  /**
   * Gets all of the Spark SQL string collations.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def collations(): Dataset[Row]

  /**
   * Gets Spark SQL keywords.
   *
   * @group generator_funcs
   * @since 4.0.0
   */
  def sql_keywords(): Dataset[Row]

  /**
   * Separates a variant object/array into multiple rows containing its fields/elements. Its
   * result schema is `struct&lt;pos int, key string, value variant&gt;`. `pos` is the position of
   * the field/element in its parent object/array, and `value` is the field/element value. `key`
   * is the field name when exploding a variant object, or is NULL when exploding a variant array.
   * It ignores any input that is not a variant array/object, including SQL NULL, variant null,
   * and any other variant values.
   *
   * @group variant_funcs
   * @since 4.0.0
   */
  def variant_explode(input: Column): Dataset[Row]

  /**
   * Separates a variant object/array into multiple rows containing its fields/elements. Its
   * result schema is `struct&lt;pos int, key string, value variant&gt;`. `pos` is the position of
   * the field/element in its parent object/array, and `value` is the field/element value. `key`
   * is the field name when exploding a variant object, or is NULL when exploding a variant array.
   * Unlike variant_explode, if the given variant is not a variant array/object, including SQL
   * NULL, variant null, and any other variant values, then NULL is produced.
   *
   * @group variant_funcs
   * @since 4.0.0
   */
  def variant_explode_outer(input: Column): Dataset[Row]
}
