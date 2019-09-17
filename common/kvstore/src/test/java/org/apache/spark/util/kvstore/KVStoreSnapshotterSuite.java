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

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class KVStoreSnapshotterSuite {
  private final KVStoreSerializer serializer = new KVStoreSerializer();
  private final KVStoreSnapshotter snapshotter = new KVStoreSnapshotter(serializer);

  @Test
  public void testMetadataEnabled() throws Exception {
    KVStore src = new InMemoryStore();
    KVStore dest = new InMemoryStore();

    CustomType1 t = new CustomType1();
    t.key = "key";
    t.id = "id";
    t.name = "name";
    src.setMetadata(t);

    prepareTestObjects(src);
    runTestSnapshotAndRestore(src, dest);
  }

  @Test
  public void testMetadataNotAvailable() throws Exception {
    KVStore src = new InMemoryStore();
    KVStore dest = new InMemoryStore();

    prepareTestObjects(src);
    runTestSnapshotAndRestore(src, dest);
  }

  private void prepareTestObjects(KVStore testStore) throws Exception {
    CustomType1 t = new CustomType1();
    t.key = "key";
    t.id = "id";
    t.name = "name";

    CustomType1 t2 = new CustomType1();
    t2.key = "key2";
    t2.id = "id";
    t2.name = "name2";

    ArrayKeyIndexType t3 = new ArrayKeyIndexType();
    t3.key = new int[] { 42, 84 };
    t3.id = new String[] { "id1", "id2" };

    IntKeyType t4 = new IntKeyType();
    t4.key = 2;
    t4.id = "2";
    t4.values = Arrays.asList("value1", "value2");

    testStore.write(t);
    testStore.write(t2);
    testStore.write(t3);
    testStore.write(t4);
  }

  // source is expected to have some metadata and objects
  // destination has to be empty
  private void runTestSnapshotAndRestore(KVStore source, KVStore destination) throws Exception {
    File snapshotFile = File.createTempFile("test-kvstore", ".snapshot");
    snapshotFile.delete();
    assertFalse(snapshotFile.exists());

    try {
      snapshotter.dump(source, snapshotFile);
      assertTrue(snapshotFile.exists() && snapshotFile.isFile());
      assertTrue(snapshotFile.length() > 0);

      snapshotter.restore(snapshotFile, destination);

      Class<?> metadataType = source.metadataType();
      assertEquals(destination.metadataType(), metadataType);
      if (metadataType != null) {
        assertEquals(destination.getMetadata(metadataType), source.getMetadata(metadataType));
      }

      Set<Class<?>> objectTypes = source.types();
      assertEquals(destination.types(), objectTypes);
      for (Class<?> tpe : objectTypes) {
        Set<Object> destObjs = new HashSet<>();
        destination.view(tpe).closeableIterator().forEachRemaining(destObjs::add);
        Set<Object> srcObjs = new HashSet<>();
        source.view(tpe).closeableIterator().forEachRemaining(srcObjs::add);
        assertEquals(destObjs, srcObjs);
      }
    } finally {
      FileUtils.deleteQuietly(snapshotFile);
    }
  }
}
