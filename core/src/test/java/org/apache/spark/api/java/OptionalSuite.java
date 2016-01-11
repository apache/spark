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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Optional}.
 */
public class OptionalSuite {

  @Test
  public void testEmpty() {
    Assert.assertFalse(Optional.empty().isPresent());
    Assert.assertNull(Optional.empty().orNull());
    Assert.assertEquals("foo", Optional.empty().or("foo"));
    Assert.assertEquals("foo", Optional.empty().orElse("foo"));
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyGet() {
    Optional.empty().get();
  }

  @Test
  public void testAbsent() {
    Assert.assertFalse(Optional.absent().isPresent());
    Assert.assertNull(Optional.absent().orNull());
    Assert.assertEquals("foo", Optional.absent().or("foo"));
    Assert.assertEquals("foo", Optional.absent().orElse("foo"));
  }

  @Test(expected = NullPointerException.class)
  public void testAbsentGet() {
    Optional.absent().get();
  }

  @Test
  public void testOf() {
    Assert.assertTrue(Optional.of(1).isPresent());
    Assert.assertNotNull(Optional.of(1).orNull());
    Assert.assertEquals(Integer.valueOf(1), Optional.of(1).get());
    Assert.assertEquals(Integer.valueOf(1), Optional.of(1).or(2));
    Assert.assertEquals(Integer.valueOf(1), Optional.of(1).orElse(2));
  }

  @Test(expected = NullPointerException.class)
  public void testOfWithNull() {
    Optional.of(null);
  }

  @Test
  public void testOfNullable() {
    Assert.assertTrue(Optional.ofNullable(1).isPresent());
    Assert.assertNotNull(Optional.ofNullable(1).orNull());
    Assert.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).get());
    Assert.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).or(2));
    Assert.assertEquals(Integer.valueOf(1), Optional.ofNullable(1).orElse(2));
    Assert.assertFalse(Optional.ofNullable(null).isPresent());
    Assert.assertNull(Optional.ofNullable(null).orNull());
    Assert.assertEquals(Integer.valueOf(2), Optional.<Integer>ofNullable(null).or(2));
    Assert.assertEquals(Integer.valueOf(2), Optional.<Integer>ofNullable(null).orElse(2));
  }

  @Test
  public void testFromNullable() {
    Assert.assertTrue(Optional.fromNullable(1).isPresent());
    Assert.assertNotNull(Optional.fromNullable(1).orNull());
    Assert.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).get());
    Assert.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).or(2));
    Assert.assertEquals(Integer.valueOf(1), Optional.fromNullable(1).orElse(2));
    Assert.assertFalse(Optional.fromNullable(null).isPresent());
    Assert.assertNull(Optional.fromNullable(null).orNull());
    Assert.assertEquals(Integer.valueOf(2), Optional.<Integer>fromNullable(null).or(2));
    Assert.assertEquals(Integer.valueOf(2), Optional.<Integer>fromNullable(null).orElse(2));
  }

}
