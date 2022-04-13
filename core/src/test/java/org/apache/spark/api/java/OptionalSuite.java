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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link Optional}.
 */
public class OptionalSuite {

  @Test
  public void testEmpty() {
    Assertions.assertFalse(Optional.empty().isPresent());
    Assertions.assertNull(Optional.empty().orNull());
    Assertions.assertEquals("foo", Optional.empty().or("foo"));
    Assertions.assertEquals("foo", Optional.empty().orElse("foo"));
  }

  @Test
  public void testEmptyGet() {
    Assertions.assertThrows(NullPointerException.class,
      () -> Optional.empty().get());
  }

  @Test
  public void testAbsent() {
    Assertions.assertFalse(Optional.absent().isPresent());
    Assertions.assertNull(Optional.absent().orNull());
    Assertions.assertEquals("foo", Optional.absent().or("foo"));
    Assertions.assertEquals("foo", Optional.absent().orElse("foo"));
  }

  @Test
  public void testAbsentGet() {
    Assertions.assertThrows(NullPointerException.class,
      () -> Optional.absent().get());
  }

  @Test
  public void testOf() {
    Assertions.assertTrue(Optional.of(1).isPresent());
    Assertions.assertNotNull(Optional.of(1).orNull());
    Assertions.assertEquals(Integer.valueOf(1), Optional.of(1).get());
    Assertions.assertEquals(Integer.valueOf(1), Optional.of(1).or(2));
    Assertions.assertEquals(Integer.valueOf(1), Optional.of(1).orElse(2));
  }

  @Test
  public void testOfWithNull() {
    Assertions.assertThrows(NullPointerException.class,
      () -> Optional.of(null));
  }

  @Test
  public void testOfNullable() {
    Assertions.assertTrue(Optional.ofNullable(1).isPresent());
    Assertions.assertNotNull(Optional.ofNullable(1).orNull());
    Assertions.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).get());
    Assertions.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).or(2));
    Assertions.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).orElse(2));
    Assertions.assertFalse(Optional.ofNullable(null).isPresent());
    Assertions.assertNull(Optional.ofNullable(null).orNull());
    Assertions.assertEquals(Integer.valueOf(2), Optional.<Integer>ofNullable(null).or(2));
    Assertions.assertEquals(Integer.valueOf(2), Optional.<Integer>ofNullable(null).orElse(2));
  }

  @Test
  public void testFromNullable() {
    Assertions.assertTrue(Optional.fromNullable(1).isPresent());
    Assertions.assertNotNull(Optional.fromNullable(1).orNull());
    Assertions.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).get());
    Assertions.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).or(2));
    Assertions.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).orElse(2));
    Assertions.assertFalse(Optional.fromNullable(null).isPresent());
    Assertions.assertNull(Optional.fromNullable(null).orNull());
    Assertions.assertEquals(Integer.valueOf(2), Optional.<Integer>fromNullable(null).or(2));
    Assertions.assertEquals(Integer.valueOf(2), Optional.<Integer>fromNullable(null).orElse(2));
  }

}
