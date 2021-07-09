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

import java.io.IOException;

/**
 * A handle on the file used when fetching remote data to disk.  Used to ensure the lifecycle of
 * writing the data, reading it back, and then cleaning it up is followed.  Specific implementations
 * may also handle encryption.  The data can be read only via DownloadFileWritableChannel,
 * which ensures data is not read until after the writer is closed.
 */
public interface DownloadFile {
  /**
   * Delete the file.
   *
   * @return  <code>true</code> if and only if the file or directory is
   *          successfully deleted; <code>false</code> otherwise
   */
  boolean delete();

  /**
   * A channel for writing data to the file.  This special channel allows access to the data for
   * reading, after the channel is closed, via {@link DownloadFileWritableChannel#closeAndRead()}.
   */
  DownloadFileWritableChannel openForWriting() throws IOException;

  /**
   * The absolute path of the file.
   */
  String path();
}
