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
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * <p>Like {@code java.util.Optional} in Java 8, {@code scala.Option} in Scala, and
 * {@code com.google.common.base.Optional} in Google Guava, this class represents a
 * value of a given type that may or may not exist. It is used in methods that wish
 * to optionally return a value, in preference to returning {@code null}.</p>
 *
 * <p>In fact, the class here is a reimplementation of the essential API of both
 * {@code java.util.Optional} and {@code com.google.common.base.Optional}. From
 * {@code java.util.Optional}, it implements:</p>
 *
 * <ul>
 *   <li>{@link #empty()}</li>
 *   <li>{@link #of(Object)}</li>
 *   <li>{@link #ofNullable(Object)}</li>
 *   <li>{@link #get()}</li>
 *   <li>{@link #orElse(Object)}</li>
 *   <li>{@link #isPresent()}</li>
 * </ul>
 *
 * <p>From {@code com.google.common.base.Optional} it implements:</p>
 *
 * <ul>
 *   <li>{@link #absent()}</li>
 *   <li>{@link #of(Object)}</li>
 *   <li>{@link #fromNullable(Object)}</li>
 *   <li>{@link #get()}</li>
 *   <li>{@link #or(Object)}</li>
 *   <li>{@link #orNull()}</li>
 *   <li>{@link #isPresent()}</li>
 * </ul>
 *
 * <p>{@code java.util.Optional} itself was not used because at the time, the
 * project did not require Java 8. Using {@code com.google.common.base.Optional}
 * has in the past caused serious library version conflicts with Guava that can't
 * be resolved by shading. Hence this work-alike clone.</p>
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

  // java.util.Optional API (subset)

  /**
   * @return an empty {@code Optional}
   */
  public static <T> Optional<T> empty() {
    @SuppressWarnings("unchecked")
    Optional<T> t = (Optional<T>) EMPTY;
    return t;
  }

  /**
   * @param value non-null value to wrap
   * @return {@code Optional} wrapping this value
   * @throws NullPointerException if value is null
   */
  public static <T> Optional<T> of(T value) {
    return new Optional<>(value);
  }

  /**
   * @param value value to wrap, which may be null
   * @return {@code Optional} wrapping this value, which may be empty
   */
  public static <T> Optional<T> ofNullable(T value) {
    if (value == null) {
      return empty();
    } else {
      return of(value);
    }
  }

  /**
   * @return the value wrapped by this {@code Optional}
   * @throws NullPointerException if this is empty (contains no value)
   */
  public T get() {
    Preconditions.checkNotNull(value);
    return value;
  }

  /**
   * @param other value to return if this is empty
   * @return this {@code Optional}'s value if present, or else the given value
   */
  public T orElse(T other) {
    return value != null ? value : other;
  }

  /**
   * @return true iff this {@code Optional} contains a value (non-empty)
   */
  public boolean isPresent() {
    return value != null;
  }

  // Guava API (subset)
  // of(), get() and isPresent() are identically present in the Guava API

  /**
   * @return an empty {@code Optional}
   */
  public static <T> Optional<T> absent() {
    return empty();
  }

  /**
   * @param value value to wrap, which may be null
   * @return {@code Optional} wrapping this value, which may be empty
   */
  public static <T> Optional<T> fromNullable(T value) {
    return ofNullable(value);
  }

  /**
   * @param other value to return if this is empty
   * @return this {@code Optional}'s value if present, or else the given value
   */
  public T or(T other) {
    return value != null ? value : other;
  }

  /**
   * @return this {@code Optional}'s value if present, or else null
   */
  public T orNull() {
    return value;
  }

  // Common methods

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Optional)) {
      return false;
    }
    Optional<?> other = (Optional<?>) obj;
    return Objects.equals(value, other.value);
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
