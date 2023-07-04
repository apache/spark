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

package org.apache.spark.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import org.apache.commons.lang3.SystemUtils;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaFileUtils {

  private static final Logger logger = LoggerFactory.getLogger(JavaFileUtils.class);

  /**
   * Create a directory inside the given parent directory with default namePrefix "spark".
   * The directory is guaranteed to be newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root) throws IOException {
    return createDirectory(root, "spark");
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root, String namePrefix) throws IOException {
    if (namePrefix == null) namePrefix = "spark";
    int attempts = 0;
    int maxAttempts = 10;
    File dir = null;
    while (dir == null) {
      attempts += 1;
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!");
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID());
        Files.createDirectories(dir.toPath());
      } catch (IOException | SecurityException e) {
        logger.error("Failed to create directory " + dir, e);
        dir = null;
      }
    }
    return dir.getCanonicalFile();
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file) throws IOException {
    deleteRecursively(file, null);
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @param filter A filename filter that make sure only files / dirs with the satisfied filenames
   *               are deleted.
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file, FilenameFilter filter) throws IOException {
    if (file == null) { return; }

    // On Unix systems, use operating system command to run faster
    // If that does not work out, fallback to the Java IO way
    if (SystemUtils.IS_OS_UNIX && filter == null) {
      try {
        deleteRecursivelyUsingUnixNative(file);
        return;
      } catch (IOException e) {
        logger.warn("Attempt to delete using native Unix OS command failed for path = {}. " +
            "Falling back to Java IO way", file.getAbsolutePath(), e);
      }
    }

    deleteRecursivelyUsingJavaIO(file, filter);
  }

  private static void deleteRecursivelyUsingJavaIO(
      File file,
      FilenameFilter filter) throws IOException {
    BasicFileAttributes fileAttributes =
      Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    if (fileAttributes.isDirectory() && !isSymlink(file)) {
      IOException savedIOException = null;
      for (File child : listFilesSafely(file, filter)) {
        try {
          deleteRecursively(child, filter);
        } catch (IOException e) {
          // In case of multiple exceptions, only last one will be thrown
          savedIOException = e;
        }
      }
      if (savedIOException != null) {
        throw savedIOException;
      }
    }

    // Delete file only when it's a normal file or an empty directory.
    if (fileAttributes.isRegularFile() ||
      (fileAttributes.isDirectory() && listFilesSafely(file, null).length == 0)) {
      boolean deleted = file.delete();
      // Delete can also fail if the file simply did not exist.
      if (!deleted && file.exists()) {
        throw new IOException("Failed to delete: " + file.getAbsolutePath());
      }
    }
  }

  private static void deleteRecursivelyUsingUnixNative(File file) throws IOException {
    ProcessBuilder builder = new ProcessBuilder("rm", "-rf", file.getAbsolutePath());
    Process process = null;
    int exitCode = -1;

    try {
      // In order to avoid deadlocks, consume the stdout (and stderr) of the process
      builder.redirectErrorStream(true);
      builder.redirectOutput(new File("/dev/null"));

      process = builder.start();

      exitCode = process.waitFor();
    } catch (Exception e) {
      throw new IOException("Failed to delete: " + file.getAbsolutePath(), e);
    } finally {
      if (process != null) {
        process.destroy();
      }
    }

    if (exitCode != 0 || file.exists()) {
      throw new IOException("Failed to delete: " + file.getAbsolutePath());
    }
  }

  private static File[] listFilesSafely(File file, FilenameFilter filter) throws IOException {
    if (file.exists()) {
      File[] files = file.listFiles(filter);
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file);
      }
      return files;
    } else {
      return new File[0];
    }
  }

  private static boolean isSymlink(File file) throws IOException {
    Preconditions.checkNotNull(file);
    File fileInCanonicalDir = null;
    if (file.getParent() == null) {
      fileInCanonicalDir = file;
    } else {
      fileInCanonicalDir = new File(file.getParentFile().getCanonicalFile(), file.getName());
    }
    return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
  }

}
