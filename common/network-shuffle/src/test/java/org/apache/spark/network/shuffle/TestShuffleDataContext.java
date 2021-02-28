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

package org.apache.spark.network.shuffle;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.io.Closeables;
import com.google.common.io.Files;

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages some sort-shuffle data, including the creation
 * and cleanup of directories that can be read by the {@link ExternalShuffleBlockResolver}.
 */
public class TestShuffleDataContext {
  private static final Logger logger = LoggerFactory.getLogger(TestShuffleDataContext.class);

  public final String[] localDirs;
  public final int subDirsPerLocalDir;

  public TestShuffleDataContext(int numLocalDirs, int subDirsPerLocalDir) {
    this.localDirs = new String[numLocalDirs];
    this.subDirsPerLocalDir = subDirsPerLocalDir;
  }

  public void create() {
    for (int i = 0; i < localDirs.length; i ++) {
      localDirs[i] = Files.createTempDir().getAbsolutePath();

      for (int p = 0; p < subDirsPerLocalDir; p ++) {
        new File(localDirs[i], String.format("%02x", p)).mkdirs();
      }
    }
  }

  public void cleanup() {
    for (String localDir : localDirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir));
      } catch (IOException e) {
        logger.warn("Unable to cleanup localDir = " + localDir, e);
      }
    }
  }

  /** Creates reducer blocks in a sort-based data format within our local dirs. */
  public void insertSortShuffleData(int shuffleId, int mapId, byte[][] blocks) throws IOException {
    String blockId = "shuffle_" + shuffleId + "_" + mapId + "_0";

    OutputStream dataStream = null;
    DataOutputStream indexStream = null;
    boolean suppressExceptionsDuringClose = true;

    try {
      dataStream = new FileOutputStream(
        ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, blockId + ".data"));
      indexStream = new DataOutputStream(new FileOutputStream(
        ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, blockId + ".index")));

      long offset = 0;
      indexStream.writeLong(offset);
      for (byte[] block : blocks) {
        offset += block.length;
        dataStream.write(block);
        indexStream.writeLong(offset);
      }
      suppressExceptionsDuringClose = false;
    } finally {
      Closeables.close(dataStream, suppressExceptionsDuringClose);
      Closeables.close(indexStream, suppressExceptionsDuringClose);
    }
  }

  /** Creates spill file(s) within the local dirs. */
  public void insertSpillData() throws IOException {
    String filename = "temp_local_uuid";
    insertFile(filename);
  }

  public void insertBroadcastData() throws IOException {
    String filename = "broadcast_12_uuid";
    insertFile(filename);
  }

  public void insertTempShuffleData() throws IOException {
    String filename = "temp_shuffle_uuid";
    insertFile(filename);
  }

  public void insertCachedRddData(int rddId, int splitId, byte[] block) throws IOException {
    String blockId = "rdd_" + rddId + "_" + splitId;
    insertFile(blockId, block);
  }

  private void insertFile(String filename) throws IOException {
    insertFile(filename, new byte[] { 42 });
  }

  private void insertFile(String filename, byte[] block) throws IOException {
    OutputStream dataStream = null;
    File file = ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, filename);
    Assert.assertFalse("this test file has been already generated", file.exists());
    try {
      dataStream = new FileOutputStream(
        ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, filename));
      dataStream.write(block);
    } finally {
      Closeables.close(dataStream, false);
    }
  }

  /**
   * Creates an ExecutorShuffleInfo object based on the given shuffle manager which targets this
   * context's directories.
   */
  public ExecutorShuffleInfo createExecutorInfo(String shuffleManager) {
    return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
  }
}
