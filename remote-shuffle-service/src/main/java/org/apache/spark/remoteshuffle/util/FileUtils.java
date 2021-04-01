/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import org.apache.spark.remoteshuffle.exceptions.RssDiskSpaceException;
import org.apache.spark.remoteshuffle.exceptions.RssFileCorruptedException;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class FileUtils {
  private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

  public static void cleanupOldFiles(String rootDir, long cutoffTimeMillis) {
    File rootDirFile = new File(rootDir);
    if (!rootDirFile.exists()) {
      return;
    }

    // delete files
    Iterator<File> iterator =
        org.apache.commons.io.FileUtils
            .iterateFiles(rootDirFile, new AgeFileFilter(cutoffTimeMillis), TrueFileFilter.TRUE);
    while (iterator.hasNext()) {
      File fileToDelete = iterator.next();
      try {
        logger.info("Deleting file: " + fileToDelete);
        fileToDelete.delete();
      } catch (Throwable ex) {
        logger.info("Failed to delete file: " + fileToDelete, ex);
      }
    }

    // delete empty directories
    for (File childFile : rootDirFile.listFiles()) {
      if (childFile.isDirectory()) {
        deleteDirRecursively(childFile);
      }
    }
  }

  public static void checkDiskFreeSpace(long minTotalDiskSpace, long minFreeDiskSpace) {
    FileStore largestFileStore = null;
    for (Path root : FileSystems.getDefault().getRootDirectories()) {
      try {
        FileStore store = Files.getFileStore(root);
        if (largestFileStore == null) {
          largestFileStore = store;
        } else if (largestFileStore.getTotalSpace() < store.getTotalSpace()) {
          largestFileStore = store;
        }
      } catch (Throwable e) {
        logger.warn(String.format("Failed to check file store size for %s", root), e);
      }
    }

    if (largestFileStore == null) {
      throw new RssDiskSpaceException("Failed to get file store");
    }

    try {
      logger.info(String
          .format("Checking file store (%s) space, total: %s, usable: %s", largestFileStore,
              largestFileStore.getTotalSpace(), largestFileStore.getUsableSpace()));
      if (largestFileStore.getTotalSpace() < minTotalDiskSpace) {
        throw new RssDiskSpaceException(String.format(
            "File store (%s) has less total space (%s) than expected (%s)",
            largestFileStore,
            largestFileStore.getTotalSpace(),
            minTotalDiskSpace));
      }

      if (largestFileStore.getUsableSpace() < minFreeDiskSpace) {
        throw new RssDiskSpaceException(String.format(
            "File store (%s) has less free space (%s) than expected (%s)",
            largestFileStore,
            largestFileStore.getUsableSpace(),
            minFreeDiskSpace));
      }
    } catch (IOException e) {
      throw new RssDiskSpaceException(
          String.format("Failed to check file store %s", largestFileStore), e);
    }
  }

  public static long getFileStoreUsableSpace() {
    long maxUsableSpace = 0;
    for (Path root : FileSystems.getDefault().getRootDirectories()) {
      try {
        FileStore store = Files.getFileStore(root);
        long storeUsableSpace = store.getUsableSpace();
        if (maxUsableSpace < storeUsableSpace) {
          maxUsableSpace = storeUsableSpace;
        }
      } catch (Throwable e) {
        logger.warn(String.format("Failed to check file store size for %s", root), e);
      }
    }
    return maxUsableSpace;
  }

  public static long getFileContentSize(String filePath) {
    try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
      return file.length();
    } catch (IOException e) {
      throw new RssFileCorruptedException(
          String.format("File to get file content size: %s", filePath), e);
    }
  }

  private static void deleteDirRecursively(File rootDir) {
    File[] childFiles = rootDir.listFiles();
    if (childFiles.length == 0) {
      try {
        logger.info("Deleting dir: " + rootDir);
        rootDir.delete();
      } catch (Throwable ex) {
        logger.info("Failed to delete dir: " + rootDir, ex);
      }
      return;
    }

    for (File childFile : childFiles) {
      if (childFile.isDirectory()) {
        deleteDirRecursively(childFile);
      }
    }
  }
}
