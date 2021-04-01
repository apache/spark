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

package org.apache.spark.remoteshuffle.storage;

import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/***
 * Local file based shuffle storage.
 */
public class ShuffleFileStorage implements ShuffleStorage {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleFileStorage.class);

  // default not using buffer, which means we depend on operation system level file cache.
  public static final int DEFAULT_BUFFER_SIZE = 0;

  public ShuffleFileStorage() {
  }

  @Override
  public boolean isLocalStorage() {
    return true;
  }

  @Override
  public boolean exists(String path) {
    return Files.exists(Paths.get(path));
  }

  @Override
  public List<String> listAllFiles(String dir) {
    try {
      return Files.walk(Paths.get(dir))
          .filter(Files::isRegularFile)
          .map(t -> t.toString()).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RssException("Failed to list directory: " + dir, e);
    }
  }

  @Override
  public void createDirectories(String dir) {
    try {
      Files.createDirectories(Paths.get(dir));
    } catch (Throwable e) {
      throw new RssException("Failed to create directories: " + dir, e);
    }
  }

  @Override
  public void deleteDirectory(String dir) {
    try {
      FileUtils.deleteDirectory(new File(dir));
    } catch (Throwable e) {
      throw new RssException("Failed to delete directory: " + dir, e);
    }
  }

  @Override
  public void deleteFile(String path) {
    try {
      new File(path).delete();
    } catch (Throwable e) {
      throw new RssException("Failed to delete file: " + path, e);
    }
  }

  @Override
  public long size(String path) {
    return org.apache.spark.remoteshuffle.util.FileUtils.getFileContentSize(path);
  }

  @Override
  public ShuffleOutputStream createWriterStream(String path, String compressionCodec) {
    // TODO remove compressionCodec from storage API
    return new ShuffleFileOutputStream(new File(path));
  }

  @Override
  public InputStream createReaderStream(String path) {
    try {
      return new FileInputStream(path);
    } catch (Throwable e) {
      throw new RssException("Failed to open file: " + path, e);
    }
  }

  @Override
  public String toString() {
    return "ShuffleFileStorage{}";
  }
}
