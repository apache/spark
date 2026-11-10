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

package org.apache.spark.sql.connector.read;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * A runtime-bound value in an offered lookup predicate, such as the {@code ?} in
 * {@code tenant = ?}; its values arrive in {@link ExternalLookupReader#lookupBatch}. Each
 * candidate has exactly one instance of its own, and equality is identity, so same-shaped
 * candidates are distinct. Instances are serializable, so a connector may keep selected
 * predicates in its {@link ExternalLookupReaderFactory}.
 *
 * @since 4.4.0
 */
@Evolving
public final class LookupParameter implements Expression, Serializable {
  private static final long serialVersionUID = 1L;
  private final DataType dataType;

  public LookupParameter(DataType dataType) {
    this.dataType = dataType;
  }

  /** Returns the value's Spark SQL data type; parameter rows use its internal representation. */
  public DataType dataType() {
    return dataType;
  }

  @Override
  public Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  @Override
  public String describe() {
    return "?";
  }

  @Override
  public String toString() {
    return describe();
  }
}
