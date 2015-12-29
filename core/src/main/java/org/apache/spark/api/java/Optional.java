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

package org.apache.spark.api.java;

import java.io.Serializable;

import com.google.common.base.Preconditions;

/**
 * API copied from {@code java.util.Optional} in Java 8 and reimplemented.
 *
 * @param <T> type of value held inside
 */
public final class Optional<T> implements Serializable {

  private static final Optional<?> EMPTY = new Optional<>();

  private final T value;

  private Optional() {
    this.value = null;
  }

  private Optional(T value) {
    Preconditions.checkNotNull(value);
    this.value = value;
  }

  public static <T> Optional<T> empty() {
    @SuppressWarnings("unchecked")
    Optional<T> t = (Optional<T>) EMPTY;
    return t;
  }


  public static <T> Optional<T> of(T value) {
    return new Optional<>(value);
  }

  public static <T> Optional<T> ofNullable(T value) {
    if (value == null) {
      return empty();
    } else {
      return of(value);
    }
  }

  public T get() {
    Preconditions.checkNotNull(value);
    return value;
  }

  public T orElse(T other) {
    return value != null ? value : other;
  }

  public boolean isPresent() {
    return value != null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Optional)) {
      return false;
    }
    Optional<?> other = (Optional<?>) obj;
    return value == null ? other.value == null : value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return value == null ? 0 : value.hashCode();
  }

  @Override
  public String toString() {
    return value == null ? "Optional.empty" : String.format("Optional[%s]", value);
  }

}
