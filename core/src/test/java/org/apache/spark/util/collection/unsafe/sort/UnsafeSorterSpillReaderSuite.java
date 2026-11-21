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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext$;
import org.apache.spark.TaskContextImpl;
import org.apache.spark.internal.config.package$;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.util.Utils;

import static org.junit.jupiter.api.Assertions.*;

public class UnsafeSorterSpillReaderSuite {

  private final SparkConf conf = new SparkConf();
  // Whether the stream of each reader created, in order, has been closed.
  private final List<AtomicBoolean> streamsClosed = new ArrayList<>();
  private final SerializerManager serializerManager = new SerializerManager(
      new JavaSerializer(conf),
      conf.clone().set(package$.MODULE$.SHUFFLE_SPILL_COMPRESS(), false)) {
    @Override
    public InputStream wrapStream(BlockId blockId, InputStream s) {
      AtomicBoolean closed = new AtomicBoolean();
      streamsClosed.add(closed);
      return new FilterInputStream(super.wrapStream(blockId, s)) {
        @Override
        public void close() throws IOException {
          closed.set(true);
          super.close();
        }
      };
    }
  };

  private File tempDir;
  private TaskContextImpl taskContext;

  @BeforeEach
  public void setUp() {
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "unsafe-test");
    taskContext = TaskContext$.MODULE$.empty();
    TaskContext$.MODULE$.setTaskContext(taskContext);
  }

  @AfterEach
  public void tearDown() {
    try {
      taskContext.markTaskCompleted(Option.empty());
    } finally {
      TaskContext$.MODULE$.unset();
      Utils.deleteRecursively(tempDir);
    }
  }

  /** A spill file in the format of `UnsafeSorterSpillWriter`, with key prefix `i` for record i. */
  private File spillFile(byte[]... records) throws IOException {
    File file = File.createTempFile("spill", ".bin", tempDir);
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
      out.writeInt(records.length);
      for (int i = 0; i < records.length; i++) {
        out.writeInt(records[i].length);
        out.writeLong(i);
        out.write(records[i]);
      }
    }
    return file;
  }

  private UnsafeSorterSpillReader reader(File file) throws IOException {
    return new UnsafeSorterSpillReader(
      serializerManager, file, new TempLocalBlockId(UUID.randomUUID()));
  }

  private static byte[] randomBytes(Random random, int length) {
    byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return bytes;
  }

  /** Waits for `ref` to be cleared, collecting garbage, and returns whether it was. */
  private static boolean collected(WeakReference<?> ref) throws InterruptedException {
    for (int i = 0; i < 100 && ref.get() != null; i++) {
      System.gc();
      Thread.sleep(10);
    }
    return ref.get() == null;
  }

  @Test
  public void readsRecordsOfAnyLength() throws IOException {
    Random random = new Random(42);
    int[] lengths = {0, 1, 1024, 1024 * 1024 + 1, 3 * 1024 * 1024, 7, 2048};
    byte[][] records = new byte[lengths.length][];
    for (int i = 0; i < lengths.length; i++) {
      records[i] = randomBytes(random, lengths[i]);
    }
    UnsafeSorterSpillReader reader = reader(spillFile(records));
    assertEquals(records.length, reader.getNumRecords());
    for (int i = 0; i < records.length; i++) {
      assertTrue(reader.hasNext());
      reader.loadNext();
      assertEquals(i, reader.getKeyPrefix());
      assertEquals(records[i].length, reader.getRecordLength());
      byte[] read = Arrays.copyOfRange((byte[]) reader.getBaseObject(), 0, records[i].length);
      assertArrayEquals(records[i], read, "record " + i);
    }
    assertFalse(reader.hasNext());
    assertTrue(streamsClosed.get(0).get());
  }

  /** Reads every record of `file` with a new reader, and returns a weak reference to it. */
  private WeakReference<UnsafeSorterSpillReader> readAll(File file) throws IOException {
    UnsafeSorterSpillReader reader = reader(file);
    while (reader.hasNext()) {
      reader.loadNext();
    }
    return new WeakReference<>(reader);
  }

  /** Reads the first record of `file` with a new reader, and returns a weak reference to it. */
  private WeakReference<UnsafeSorterSpillReader> readFirst(File file) throws IOException {
    UnsafeSorterSpillReader reader = reader(file);
    reader.loadNext();
    return new WeakReference<>(reader);
  }

  @Test
  public void readerDoneWithIsCollectedBeforeTheTaskEnds() throws Exception {
    // The reader closes its stream after the last record. The task completion listener that
    // closes the stream of an abandoned reader must not keep the reader, and its record buffer,
    // until the task ends: a sort-merge join whose matches spilled creates a reader for every
    // streamed row.
    Random random = new Random(42);
    WeakReference<UnsafeSorterSpillReader> ref =
      readAll(spillFile(randomBytes(random, 100), randomBytes(random, 1024 * 1024)));
    assertTrue(streamsClosed.get(0).get());
    assertTrue(collected(ref), "the reader was kept after it was done with");
  }

  @Test
  public void abandonedReaderIsCollectedAndItsStreamClosedAtTaskEnd() throws Exception {
    Random random = new Random(42);
    WeakReference<UnsafeSorterSpillReader> ref =
      readFirst(spillFile(randomBytes(random, 100), randomBytes(random, 100)));
    assertFalse(streamsClosed.get(0).get());
    assertTrue(collected(ref), "the abandoned reader was kept");
    assertFalse(streamsClosed.get(0).get());
    taskContext.markTaskCompleted(Option.empty());
    assertTrue(streamsClosed.get(0).get(), "the abandoned reader's stream was left open");
  }
}
