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

package org.apache.hadoop.fs;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;

public class TestChecksumFileSystem extends TestCase {
  public void testgetChecksumLength() throws Exception {
    assertEquals(8, ChecksumFileSystem.getChecksumLength(0L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(1L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(512L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(513L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1023L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1024L, 512));
    assertEquals(408, ChecksumFileSystem.getChecksumLength(100L, 1));
    assertEquals(4000000000008L,
                 ChecksumFileSystem.getChecksumLength(10000000000000L, 10));    
  } 
  
  // cehck that the checksum file is deleted for Checksum file system.
  public void testDeletionOfCheckSum() throws Exception {
    Configuration conf = new Configuration();
    URI uri = URI.create("ramfs://mapoutput" + "_tmp");
    InMemoryFileSystem inMemFs =  (InMemoryFileSystem)FileSystem.get(uri, conf);
    Path testPath = new Path("/file_1");
    inMemFs.reserveSpaceWithCheckSum(testPath, 1024);
    FSDataOutputStream fout = inMemFs.create(testPath);
    fout.write("testing".getBytes());
    fout.close();
    assertTrue("checksum exists", inMemFs.exists(inMemFs.getChecksumFile(testPath)));
    inMemFs.delete(testPath, true);
    assertTrue("checksum deleted", !inMemFs.exists(inMemFs.getChecksumFile(testPath)));
    // check for directories getting deleted.
    testPath = new Path("/tesdir/file_1");
    inMemFs.reserveSpaceWithCheckSum(testPath, 1024);
    fout = inMemFs.create(testPath);
    fout.write("testing".getBytes());
    fout.close();
    testPath = new Path("/testdir/file_2");
    inMemFs.reserveSpaceWithCheckSum(testPath, 1024);
    fout = inMemFs.create(testPath);
    fout.write("testing".getBytes());
    fout.close();
    inMemFs.delete(testPath, true);
    assertTrue("nothing in the namespace", inMemFs.listStatus(new Path("/")).length == 0);
  }
  
  public void testVerifyChecksum() throws Exception {
    String TEST_ROOT_DIR
    = System.getProperty("test.build.data","build/test/data/work-dir/localfs");
    
    Configuration conf = new Configuration();
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    Path testPath = new Path(TEST_ROOT_DIR, "testPath");
    Path testPath11 = new Path(TEST_ROOT_DIR, "testPath11");
    FSDataOutputStream fout = localFs.create(testPath);
    fout.write("testing".getBytes());
    fout.close();
    
    fout = localFs.create(testPath11);
    fout.write("testing you".getBytes());
    fout.close();
    
    localFs.delete(localFs.getChecksumFile(testPath), true);
    assertTrue("checksum deleted", !localFs.exists(localFs.getChecksumFile(testPath)));
    
    //copying the wrong checksum file
    FileUtil.copy(localFs, localFs.getChecksumFile(testPath11), localFs, 
        localFs.getChecksumFile(testPath),false,true,conf);
    assertTrue("checksum exists", localFs.exists(localFs.getChecksumFile(testPath)));
    
    boolean errorRead = false;
    try {
      TestLocalFileSystem.readFile(localFs, testPath);
    }catch(ChecksumException ie) {
      errorRead = true;
    }
    assertTrue("error reading", errorRead);
    
    //now setting verify false, the read should succeed
    localFs.setVerifyChecksum(false);
    String str = TestLocalFileSystem.readFile(localFs, testPath);
    assertTrue("read", "testing".equals(str));
    
  }
}
