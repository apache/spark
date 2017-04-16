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

package org.apache.hadoop.streaming;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

class UtilTest {

  /**
   * Utility routine to recurisvely delete a directory.
   * On normal return, the file does not exist.
   *
   * @param file File or directory to delete.
   *
   * @throws RuntimeException if the file, or some file within
   * it, could not be deleted.
   */
  static void recursiveDelete(File file) {
    file = file.getAbsoluteFile();

    if (!file.exists()) return;
    
    if (file.isDirectory()) {
      for (File child : file.listFiles()) {
	recursiveDelete(child);
      }
    }
    if (!file.delete()) {
      throw new RuntimeException("Failed to delete " + file);
    }
  }
  
  public UtilTest(String testName) {
    testName_ = testName;
    userDir_ = System.getProperty("user.dir");
    antTestDir_ = System.getProperty("test.build.data", userDir_);
    System.out.println("test.build.data-or-user.dir=" + antTestDir_);
  }

  void checkUserDir() {
    // trunk/src/contrib/streaming --> trunk/build/contrib/streaming/test/data
    if (!userDir_.equals(antTestDir_)) {
      // because changes to user.dir are ignored by File static methods.
      throw new IllegalStateException("user.dir != test.build.data. The junit Ant task must be forked.");
    }
  }

  void redirectIfAntJunit() throws IOException
  {
    boolean fromAntJunit = System.getProperty("test.build.data") != null;
    if (fromAntJunit) {
      new File(antTestDir_).mkdirs();
      File outFile = new File(antTestDir_, testName_+".log");
      PrintStream out = new PrintStream(new FileOutputStream(outFile));
      System.setOut(out);
      System.setErr(out);
    }
  }

  private String userDir_;
  private String antTestDir_;
  private String testName_;
}
