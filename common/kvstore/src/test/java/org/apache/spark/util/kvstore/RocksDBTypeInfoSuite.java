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

package org.apache.spark.util.kvstore;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.Test;
import static org.junit.Assert.*;

public class RocksDBTypeInfoSuite {

  @Test
  public void testIndexAnnotation() throws Exception {
    KVTypeInfo ti = new KVTypeInfo(CustomType1.class);
    assertEquals(5, ti.indices().count());

    CustomType1 t1 = new CustomType1();
    t1.key = "key";
    t1.id = "id";
    t1.name = "name";
    t1.num = 42;
    t1.child = "child";

    assertEquals(t1.key, ti.getIndexValue(KVIndex.NATURAL_INDEX_NAME, t1));
    assertEquals(t1.id, ti.getIndexValue("id", t1));
    assertEquals(t1.name, ti.getIndexValue("name", t1));
    assertEquals(t1.num, ti.getIndexValue("int", t1));
    assertEquals(t1.child, ti.getIndexValue("child", t1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoNaturalIndex() throws Exception {
    newTypeInfo(NoNaturalIndex.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoNaturalIndex2() throws Exception {
    newTypeInfo(NoNaturalIndex2.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateIndex() throws Exception {
    newTypeInfo(DuplicateIndex.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIndexName() throws Exception {
    newTypeInfo(EmptyIndexName.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalIndexName() throws Exception {
    newTypeInfo(IllegalIndexName.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalIndexMethod() throws Exception {
    newTypeInfo(IllegalIndexMethod.class);
  }

  @Test
  public void testKeyClashes() throws Exception {
    RocksDBTypeInfo ti = newTypeInfo(CustomType1.class);

    CustomType1 t1 = new CustomType1();
    t1.key = "key1";
    t1.name = "a";

    CustomType1 t2 = new CustomType1();
    t2.key = "key2";
    t2.name = "aa";

    CustomType1 t3 = new CustomType1();
    t3.key = "key3";
    t3.name = "aaa";

    // Make sure entries with conflicting names are sorted correctly.
    assertBefore(ti.index("name").entityKey(null, t1), ti.index("name").entityKey(null, t2));
    assertBefore(ti.index("name").entityKey(null, t1), ti.index("name").entityKey(null, t3));
    assertBefore(ti.index("name").entityKey(null, t2), ti.index("name").entityKey(null, t3));
  }

  @Test
  public void testNumEncoding() throws Exception {
    RocksDBTypeInfo.Index idx = newTypeInfo(CustomType1.class).indices().iterator().next();

    assertEquals("+=00000001", new String(idx.toKey(1), UTF_8));
    assertEquals("+=00000010", new String(idx.toKey(16), UTF_8));
    assertEquals("+=7fffffff", new String(idx.toKey(Integer.MAX_VALUE), UTF_8));

    assertBefore(idx.toKey(1), idx.toKey(2));
    assertBefore(idx.toKey(-1), idx.toKey(2));
    assertBefore(idx.toKey(-11), idx.toKey(2));
    assertBefore(idx.toKey(-11), idx.toKey(-1));
    assertBefore(idx.toKey(1), idx.toKey(11));
    assertBefore(idx.toKey(Integer.MIN_VALUE), idx.toKey(Integer.MAX_VALUE));

    assertBefore(idx.toKey(1L), idx.toKey(2L));
    assertBefore(idx.toKey(-1L), idx.toKey(2L));
    assertBefore(idx.toKey(Long.MIN_VALUE), idx.toKey(Long.MAX_VALUE));

    assertBefore(idx.toKey((short) 1), idx.toKey((short) 2));
    assertBefore(idx.toKey((short) -1), idx.toKey((short) 2));
    assertBefore(idx.toKey(Short.MIN_VALUE), idx.toKey(Short.MAX_VALUE));

    assertBefore(idx.toKey((byte) 1), idx.toKey((byte) 2));
    assertBefore(idx.toKey((byte) -1), idx.toKey((byte) 2));
    assertBefore(idx.toKey(Byte.MIN_VALUE), idx.toKey(Byte.MAX_VALUE));

    byte prefix = RocksDBTypeInfo.ENTRY_PREFIX;
    assertSame(new byte[] { prefix, RocksDBTypeInfo.FALSE }, idx.toKey(false));
    assertSame(new byte[] { prefix, RocksDBTypeInfo.TRUE }, idx.toKey(true));
  }

  @Test
  public void testArrayIndices() throws Exception {
    RocksDBTypeInfo.Index idx = newTypeInfo(CustomType1.class).indices().iterator().next();

    assertBefore(idx.toKey(new String[] { "str1" }), idx.toKey(new String[] { "str2" }));
    assertBefore(idx.toKey(new String[] { "str1", "str2" }),
      idx.toKey(new String[] { "str1", "str3" }));

    assertBefore(idx.toKey(new int[] { 1 }), idx.toKey(new int[] { 2 }));
    assertBefore(idx.toKey(new int[] { 1, 2 }), idx.toKey(new int[] { 1, 3 }));
  }

  private RocksDBTypeInfo newTypeInfo(Class<?> type) throws Exception {
    return new RocksDBTypeInfo(null, type, type.getName().getBytes(UTF_8));
  }

  private void assertBefore(byte[] key1, byte[] key2) {
    assertBefore(new String(key1, UTF_8), new String(key2, UTF_8));
  }

  private void assertBefore(String str1, String str2) {
    assertTrue(String.format("%s < %s failed", str1, str2), str1.compareTo(str2) < 0);
  }

  private void assertSame(byte[] key1, byte[] key2) {
    assertEquals(new String(key1, UTF_8), new String(key2, UTF_8));
  }

  public static class NoNaturalIndex {

    public String id;

  }

  public static class NoNaturalIndex2 {

    @KVIndex("id")
    public String id;

  }

  public static class DuplicateIndex {

    @KVIndex
    public String key;

    @KVIndex("id")
    public String id;

    @KVIndex("id")
    public String id2;

  }

  public static class EmptyIndexName {

    @KVIndex("")
    public String id;

  }

  public static class IllegalIndexName {

    @KVIndex("__invalid")
    public String id;

  }

  public static class IllegalIndexMethod {

    @KVIndex("id")
    public String id(boolean illegalParam) {
      return null;
    }

  }

}
