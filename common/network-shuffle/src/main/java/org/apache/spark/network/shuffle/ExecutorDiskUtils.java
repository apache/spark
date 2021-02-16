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

import java.io.File;

import org.apache.spark.network.util.JavaUtils;

public class ExecutorDiskUtils {

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  public static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
    int hash = JavaUtils.nonNegativeHash(filename);
    String localDir = localDirs[hash % localDirs.length];
    int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
    final String notNormalizedPath =
      localDir + File.separator + String.format("%02x", subDirId) + File.separator + filename;
    // Interning the normalized path as according to measurements, in some scenarios such
    // duplicate strings may waste a lot of memory (~ 10% of the heap).
    // Unfortunately, we cannot just call the normalization code that java.io.File
    // uses, since it is in the package-private class java.io.FileSystem.
    // So we are creating a File just to get the normalized path back to intern it.
    // Finally a new File is built and returned with this interned normalized path.
    final String normalizedInternedPath = new File(notNormalizedPath).getPath().intern();
    return new File(normalizedInternedPath);
  }

}
