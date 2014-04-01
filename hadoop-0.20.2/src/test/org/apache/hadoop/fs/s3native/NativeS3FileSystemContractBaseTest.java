/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3native;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem.NativeS3FsInputStream;

public abstract class NativeS3FileSystemContractBaseTest
  extends FileSystemContractBaseTest {
  
  private NativeFileSystemStore store;
  
  abstract NativeFileSystemStore getNativeFileSystemStore() throws IOException;

  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    store = getNativeFileSystemStore();
    fs = new NativeS3FileSystem(store);
    fs.initialize(URI.create(conf.get("test.fs.s3n.name")), conf);
  }
  
  @Override
  protected void tearDown() throws Exception {
    store.purge("test");
    super.tearDown();
  }
  
  public void testListStatusForRoot() throws Exception {
    Path testDir = path("/test");
    assertTrue(fs.mkdirs(testDir));
    
    FileStatus[] paths = fs.listStatus(path("/"));
    assertEquals(1, paths.length);
    assertEquals(path("/test"), paths[0].getPath());
  }
  
  public void testNoTrailingBackslashOnBucket() throws Exception {
    assertTrue(fs.getFileStatus(new Path(fs.getUri().toString())).isDir());
  }
  
  public void testBlockSize() throws Exception {
    Path file = path("/test/hadoop/file");
    createFile(file);
    assertEquals("Default block size", fs.getDefaultBlockSize(),
    fs.getFileStatus(file).getBlockSize());

    // Block size is determined at read time
    long newBlockSize = fs.getDefaultBlockSize() * 2;
    fs.getConf().setLong("fs.s3n.block.size", newBlockSize);
    assertEquals("Double default block size", newBlockSize,
    fs.getFileStatus(file).getBlockSize());
  }


  private void createTestFiles(String base) throws IOException {
    store.storeEmptyFile(base + "/file1");
    store.storeEmptyFile(base + "/dir/file2");
    store.storeEmptyFile(base + "/dir/file3");
  }

  public void testDirWithDifferentMarkersWorks() throws Exception {

    for (int i = 0; i < 3; i++) {
      String base = "test/hadoop" + i;
      Path path = path("/" + base);

      createTestFiles(base);

      if (i == 0 ) {
        //do nothing, we are testing correctness with no markers
      }
      else if (i == 1) {
        // test for _$folder$ marker
        store.storeEmptyFile(base + "_$folder$");
        store.storeEmptyFile(base + "/dir_$folder$");
      }
      else if (i == 2) {
        // test the end slash file marker
        store.storeEmptyFile(base + "/");
        store.storeEmptyFile(base + "/dir/");
      }
      else if (i == 3) {
        // test both markers
        store.storeEmptyFile(base + "_$folder$");
        store.storeEmptyFile(base + "/dir_$folder$");
        store.storeEmptyFile(base + "/");
        store.storeEmptyFile(base + "/dir/");
      }

      assertTrue(fs.getFileStatus(path).isDir());
      assertEquals(2, fs.listStatus(path).length);
    }
  }

  public void testDeleteWithNoMarker() throws Exception {
    String base = "test/hadoop";
    Path path = path("/" + base);

    createTestFiles(base);

    fs.delete(path, true);

    path = path("/test");
    assertTrue(fs.getFileStatus(path).isDir());
    assertEquals(0, fs.listStatus(path).length);
  }

  public void testRenameWithNoMarker() throws Exception {
    String base = "test/hadoop";
    Path dest = path("/test/hadoop2");

    createTestFiles(base);

    fs.rename(path("/" + base), dest);

    Path path = path("/test");
    assertTrue(fs.getFileStatus(path).isDir());
    assertEquals(1, fs.listStatus(path).length);
    assertTrue(fs.getFileStatus(dest).isDir());
    assertEquals(2, fs.listStatus(dest).length);
  }

  public void testEmptyFile() throws Exception {
    store.storeEmptyFile("test/hadoop/file1");
    fs.open(path("/test/hadoop/file1")).close();
  }
}
