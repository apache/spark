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

package org.apache.spark.launcher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link FilteredObjectInputStream} - the SPARK-20922 allow-list
 * guarding the launcher's local socket protocol. Covers the security-relevant
 * {@code resolveClass} path that previously had zero direct tests.
 */
public class FilteredObjectInputStreamSuite extends BaseSuite {

  @Test
  public void testAllowedJavaLangStringIsAccepted() throws Exception {
    String original = "hello";
    Object deserialized = roundTrip(original);
    assertEquals(original, deserialized);
  }

  @Test
  public void testAllowedJavaLangIntegerIsAccepted() throws Exception {
    Integer original = 42;
    Object deserialized = roundTrip(original);
    assertEquals(original, deserialized);
  }

  @Test
  public void testAllowedLauncherMessageIsAccepted() throws Exception {
    LauncherProtocol.Hello original = new LauncherProtocol.Hello("secret", "3.5.0");
    LauncherProtocol.Hello deserialized =
        (LauncherProtocol.Hello) roundTrip(original);
    assertEquals(original.secret, deserialized.secret);
    assertEquals(original.sparkVersion, deserialized.sparkVersion);
  }

  @Test
  public void testAllowedLauncherSetAppIdIsAccepted() throws Exception {
    LauncherProtocol.SetAppId original = new LauncherProtocol.SetAppId("app-123");
    LauncherProtocol.SetAppId deserialized =
        (LauncherProtocol.SetAppId) roundTrip(original);
    assertEquals(original.appId, deserialized.appId);
  }

  @Test
  public void testDisallowedHashMapIsRejected() throws Exception {
    HashMap<String, String> payload = new HashMap<>();
    payload.put("k", "v");
    byte[] bytes = serialize(payload);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> deserializeFiltered(bytes));
    assertTrue(thrown.getMessage().contains("Unexpected class in stream"));
    assertTrue(thrown.getMessage().contains("java.util.HashMap"));
  }

  @Test
  public void testDisallowedArrayListIsRejected() throws Exception {
    java.util.ArrayList<String> payload = new java.util.ArrayList<>();
    payload.add("a");
    byte[] bytes = serialize(payload);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> deserializeFiltered(bytes));
    assertTrue(thrown.getMessage().contains("Unexpected class in stream"));
    assertTrue(thrown.getMessage().contains("java.util.ArrayList"));
  }

  @Test
  public void testDisallowedCustomClassIsRejected() throws Exception {
    // java.io.File is Serializable but lives in java.io, not in the allow-list.
    java.io.File payload = new java.io.File("/tmp/evil");
    byte[] bytes = serialize(payload);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> deserializeFiltered(bytes));
    assertTrue(thrown.getMessage().contains("Unexpected class in stream"));
    assertTrue(thrown.getMessage().contains("java.io.File"));
  }

  // Helpers

  private static byte[] serialize(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    return bos.toByteArray();
  }

  private static Object deserializeFiltered(byte[] bytes) throws Exception {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    try (FilteredObjectInputStream in = new FilteredObjectInputStream(bis)) {
      return in.readObject();
    }
  }

  private static Object roundTrip(Object obj) throws Exception {
    return deserializeFiltered(serialize(obj));
  }
}
