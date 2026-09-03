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
import java.io.File;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.launcherMalicious.LauncherPrefixSpoof;
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
    ArrayList<String> payload = new ArrayList<>();
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
    // File is Serializable but lives in java.io, not in the allow-list.
    File payload = new File("/tmp/evil");
    byte[] bytes = serialize(payload);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> deserializeFiltered(bytes));
    assertTrue(thrown.getMessage().contains("Unexpected class in stream"));
    assertTrue(thrown.getMessage().contains("java.io.File"));
  }

  // ALLOWED_PACKAGES entries end in a literal dot, so a class merely sharing the
  // "org.apache.spark.launcher" text without being in that package must still be
  // rejected: LauncherPrefixSpoof (org.apache.spark.launcherMalicious.LauncherPrefixSpoof)
  // pins down that boundary, since startsWith("org.apache.spark.launcher.") is false
  // once the character after the prefix is "M" rather than ".". A matching spoof of
  // the "java.lang." prefix (e.g. java.langfoo.Bar) can't be tested the same way: the
  // JVM refuses to define any class whose package starts with "java.", so no real
  // Class backs that name.
  @Test
  public void testDisallowedLauncherPrefixSpoofIsRejected() throws Exception {
    LauncherPrefixSpoof payload = new LauncherPrefixSpoof();
    byte[] bytes = serialize(payload);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> deserializeFiltered(bytes));
    assertTrue(thrown.getMessage().contains("Unexpected class in stream"));
    assertTrue(thrown.getMessage().contains(
        "org.apache.spark.launcherMalicious.LauncherPrefixSpoof"));
  }

  // The three tests below document CURRENT resolveClass behavior for classes in
  // java.lang.* subpackages (java.lang.reflect, java.lang.invoke, java.lang.ref).
  // desc.getName().startsWith("java.lang.") matches these too, since a subpackage's
  // fully-qualified name still starts with the literal string "java.lang." - not just
  // the java.lang package itself. The original SPARK-20922 PR's stated intent was "just
  // two packages" (an exact-package match), so this is a real gap between intent and
  // implementation, not a design choice made in this PR; fixing resolveClass itself is
  // intentionally out of scope here (see SPARK-58785 discussion) and left for a follow-up.
  //
  // None of Field, MethodHandle, or WeakReference are actually serializable (constructing
  // them for a round-trip throws NotSerializableException), so this boundary can't be
  // exercised the way the tests above are; resolveClass is called directly against a
  // synthetic descriptor instead. That gap in reachability - not just missing tests - is
  // why this went uncovered by LauncherServerSuite's indirect coverage for 9 years.

  @Test
  public void testJavaLangReflectFieldIsCurrentlyAllowed() throws Exception {
    ObjectStreamClass desc = ObjectStreamClass.lookupAny(java.lang.reflect.Field.class);
    try (FilteredObjectInputStream in = newFilteredStream()) {
      assertEquals(java.lang.reflect.Field.class, in.resolveClass(desc));
    }
  }

  @Test
  public void testJavaLangInvokeMethodHandleIsCurrentlyAllowed() throws Exception {
    ObjectStreamClass desc = ObjectStreamClass.lookupAny(java.lang.invoke.MethodHandle.class);
    try (FilteredObjectInputStream in = newFilteredStream()) {
      assertEquals(java.lang.invoke.MethodHandle.class, in.resolveClass(desc));
    }
  }

  @Test
  public void testJavaLangRefWeakReferenceIsCurrentlyAllowed() throws Exception {
    ObjectStreamClass desc = ObjectStreamClass.lookupAny(java.lang.ref.WeakReference.class);
    try (FilteredObjectInputStream in = newFilteredStream()) {
      assertEquals(java.lang.ref.WeakReference.class, in.resolveClass(desc));
    }
  }

  // Helpers

  private static FilteredObjectInputStream newFilteredStream() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // An empty ObjectOutputStream still writes a valid stream header on construction,
    // which is all FilteredObjectInputStream's constructor needs; resolveClass is called
    // directly below rather than via readObject(), so no payload is required here.
    new ObjectOutputStream(bos).close();
    return new FilteredObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
  }

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
