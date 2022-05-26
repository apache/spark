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
package org.apache.spark.network.util;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.*;

public class JavaUtilsSuite {

  @Test
  public void testCreateDirectory() throws IOException {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File testDir = new File(tmpDir, "createDirectory" + System.nanoTime());
    String testDirPath = testDir.getCanonicalPath();

    // 1. Directory created successfully
    assertTrue(JavaUtils.createDirectory(testDirPath, "scenario1").exists());

    // 2. Illegal file path
    StringBuilder namePrefix = new StringBuilder();
    for (int i = 0; i < 256; i++) {
      namePrefix.append("scenario2");
    }
    assertThrows(IOException.class,
      () -> JavaUtils.createDirectory(testDirPath, namePrefix.toString()));

    // 3. The parent directory cannot read
    assertTrue(testDir.canRead());
    assertTrue(testDir.setReadable(false));
    assertTrue(JavaUtils.createDirectory(testDirPath, "scenario3").exists());
    assertTrue(testDir.setReadable(true));

    // 4. The parent directory cannot write
    assertTrue(testDir.canWrite());
    assertTrue(testDir.setWritable(false));
    assertThrows(IOException.class,
      () -> JavaUtils.createDirectory(testDirPath, "scenario4"));
    assertTrue(testDir.setWritable(true));
  }
}
