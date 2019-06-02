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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.network.util.JavaUtils;

public class ExecutorDiskUtils {

  private static final Pattern MULTIPLE_SEPARATORS = Pattern.compile(File.separator + "{2,}");

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  public static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
    int hash = JavaUtils.nonNegativeHash(filename);
    String localDir = localDirs[hash % localDirs.length];
    int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
    return new File(createNormalizedInternedPathname(
        localDir, String.format("%02x", subDirId), filename));
  }

  /**
   * This method is needed to avoid the situation when multiple File instances for the
   * same pathname "foo/bar" are created, each with a separate copy of the "foo/bar" String.
   * According to measurements, in some scenarios such duplicate strings may waste a lot
   * of memory (~ 10% of the heap). To avoid that, we intern the pathname, and before that
   * we make sure that it's in a normalized form (contains no "//", "///" etc.) Otherwise,
   * the internal code in java.io.File would normalize it later, creating a new "foo/bar"
   * String copy. Unfortunately, we cannot just reuse the normalization code that java.io.File
   * uses, since it is in the package-private class java.io.FileSystem.
   */
  @VisibleForTesting
  static String createNormalizedInternedPathname(String dir1, String dir2, String fname) {
    String pathname = dir1 + File.separator + dir2 + File.separator + fname;
    Matcher m = MULTIPLE_SEPARATORS.matcher(pathname);
    pathname = m.replaceAll("/");
    // A single trailing slash needs to be taken care of separately
    if (pathname.length() > 1 && pathname.endsWith("/")) {
      pathname = pathname.substring(0, pathname.length() - 1);
    }
    return pathname.intern();
  }

}
