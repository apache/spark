/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
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

import java.io.InputStream;
import java.util.List;

/***
 * Shuffle storage interface.
 */
public interface ShuffleStorage {

  /***
   * Check whether this is local storage on Remote Shuffle Service, or
   * external storage like HDFS.
   * @return
   */
  boolean isLocalStorage();

  /***
   * Check whether the file exists.
   * @param path
   * @return
   */
  boolean exists(String path);

  /***
   * List all files under a directory.
   * @param dir
   * @return
   */
  List<String> listAllFiles(String dir);

  /***
   * Create directory and its parents.
   * @param dir
   */
  void createDirectories(String dir);

  /***
   * Delete directory and its children.
   * @param dir
   */
  void deleteDirectory(String dir);

  /***
   * Delete file.
   * @param path
   */
  void deleteFile(String path);

  /***
   * Get the size of the file.
   * @param path
   * @return
   */
  long size(String path);

  /***
   * Create a stream for a given file path to write shuffle data.
   * @param path
   * @param compressionCodec
   * @return
   */
  ShuffleOutputStream createWriterStream(String path, String compressionCodec);

  /***
   * Create a stream for a given file path to read shuffle data.
   * @param path
   * @return
   */
  InputStream createReaderStream(String path);
}
