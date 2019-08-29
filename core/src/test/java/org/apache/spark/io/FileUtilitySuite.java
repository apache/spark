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
package org.apache.spark.io;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Tests functionality of {@link FileUtility}
 */
public class FileUtilitySuite {

  protected File sourceFolder;
  protected File destTarLoc;
  protected File destFolder;

  @Before
  public void setUp() throws IOException {
    String tmpDir = System.getProperty("java.io.tmpdir");
    sourceFolder = Utils.createTempDir(tmpDir, "FileUtilTest-src-" + RandomUtils.nextLong());
    destFolder = Utils.createTempDir(tmpDir, "FileUtilTest-dest-" + RandomUtils.nextLong());
    destTarLoc= File.createTempFile("dest-tar", ".tar");
  }

  @After
  public void tearDown() {
    destTarLoc.delete();
  }

  @Test
  public void testCreationAndExtraction() throws IllegalStateException, IOException {
    // Create a temp file in the source folder
    Assert.assertEquals(sourceFolder.listFiles().length , 0);
    File inputFile = File.createTempFile("source-file", ".tmp", sourceFolder);
    // Create a byte array of size 1 KB with random bytes
    byte[] randomBytes =  RandomUtils.nextBytes(1 * 1024);
    FileUtils.writeByteArrayToFile(inputFile, randomBytes);

    // Create the tarball
    destTarLoc.delete();
    Assert.assertFalse(destTarLoc.exists());
    FileUtility.createTarFile(sourceFolder.toString(), destTarLoc.getAbsolutePath());
    Assert.assertTrue(destTarLoc.exists());

    // Extract the tarball
    Assert.assertEquals(destFolder.listFiles().length , 0);
    FileUtility.extractTarFile(destTarLoc.getAbsolutePath(), destFolder.getAbsolutePath());

    // Verify that the extraction was successful
    Assert.assertTrue(destFolder.exists());
    Assert.assertEquals(destFolder.listFiles().length , 1);
    Assert.assertArrayEquals(randomBytes, FileUtils.readFileToByteArray(destFolder.listFiles()[0]));
  }

}
