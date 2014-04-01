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
package org.apache.hadoop.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestGenericOptionsParser extends TestCase {
  File testDir;
  Configuration conf;
  FileSystem localFs;
      
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    testDir = new File(System.getProperty("test.build.data", "/tmp"), "generic");
    if(testDir.exists())
      localFs.delete(new Path(testDir.toString()), true);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if(testDir.exists()) {
      localFs.delete(new Path(testDir.toString()), true);
    }
  }

  /**
   * testing -fileCache option
   * @throws IOException
   */
  public void testTokenCacheOption() throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    
    File tmpFile = new File(testDir, "tokenCacheFile");
    if(tmpFile.exists()) {
      tmpFile.delete();
    }
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-tokenCacheFile";
    args[1] = tmpFile.toString();
    
    // test non existing file
    Throwable th = null;
    try {
      new GenericOptionsParser(conf, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull(th);
    
    // create file
    Path tmpPath = new Path(tmpFile.toString());
    localFs.create(tmpPath);
    new GenericOptionsParser(conf, args);
    String fileName = conf.get("mapreduce.job.credentials.json");
    assertNotNull("files is null", fileName);
    assertEquals("files option does not match",
      localFs.makeQualified(tmpPath).toString(), fileName);
    
    localFs.delete(new Path(testDir.getAbsolutePath()), true);
  }
  
  public void testFilesOption() throws Exception {
    File tmpFile = new File(testDir, "tmpfile");
    Path tmpPath = new Path(tmpFile.toString());
    localFs.create(tmpPath);
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-files";
    args[1] = tmpFile.toString();
    new GenericOptionsParser(conf, args);
    String files = conf.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match",
      localFs.makeQualified(tmpPath).toString(), files);
    
    // pass file as uri
    Configuration conf1 = new Configuration();
    URI tmpURI = new URI(tmpFile.toString() + "#link");
    args[0] = "-files";
    args[1] = tmpURI.toString();
    new GenericOptionsParser(conf1, args);
    files = conf1.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match", 
      localFs.makeQualified(new Path(tmpURI)).toString(), files);
   
    // pass a file that does not exist.
    // GenericOptionParser should throw exception
    Configuration conf2 = new Configuration();
    args[0] = "-files";
    args[1] = "file:///xyz.txt";
    Throwable th = null;
    try {
      new GenericOptionsParser(conf2, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull("throwable is null", th);
    assertTrue("FileNotFoundException is not thrown",
      th instanceof FileNotFoundException);
    files = conf2.get("tmpfiles");
    assertNull("files is not null", files);
  }

}
