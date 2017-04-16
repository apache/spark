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
package org.apache.hadoop.io;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class TestSecureIOUtils {
  private static String realOwner; 
  private static final File testFilePath =
      new File(System.getProperty("test.build.data"), "TestSecureIOContext");

  @BeforeClass
  public static void makeTestFile() throws Exception {
    FileOutputStream fos = new FileOutputStream(testFilePath);
    fos.write("hello".getBytes("UTF-8"));
    fos.close();

    Configuration conf = new Configuration();
    FileSystem rawFS = FileSystem.getLocal(conf).getRaw();
    FileStatus stat = rawFS.getFileStatus(
      new Path(testFilePath.toString()));
    realOwner = stat.getOwner();
  }

  @Test
  public void testReadUnrestricted() throws IOException {
    SecureIOUtils.openForRead(testFilePath, null).close();
  }

  @Test
  public void testReadCorrectlyRestrictedWithSecurity() throws IOException {
    SecureIOUtils
      .openForRead(testFilePath, realOwner).close();
  }

  @Test
  public void testReadIncorrectlyRestrictedWithSecurity() throws IOException {
    // this will only run if libs are available
    assumeTrue(NativeIO.isAvailable());

    System.out.println("Running test with native libs...");

    try {
      SecureIOUtils
        .forceSecureOpenForRead(testFilePath, "invalidUser").close();
      fail("Didn't throw expection for wrong ownership!");
    } catch (IOException ioe) {
      // expected
    }
  }

  @Test
  public void testCreateForWrite() throws IOException {
    try {
      SecureIOUtils.createForWrite(testFilePath, 0777);
      fail("Was able to create file at " + testFilePath);
    } catch (SecureIOUtils.AlreadyExistsException aee) {
      // expected
    }
  }
}
