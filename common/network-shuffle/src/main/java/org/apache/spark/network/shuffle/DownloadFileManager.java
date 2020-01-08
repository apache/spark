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

import org.apache.spark.network.util.TransportConf;

/**
 * A manager to create temp block files used when fetching remote data to reduce the memory usage.
 * It will clean files when they won't be used any more.
 */
public interface DownloadFileManager {

  /** Create a temp block file. */
  DownloadFile createTempFile(TransportConf transportConf);

  /**
   * Register a temp file to clean up when it won't be used any more. Return whether the
   * file is registered successfully. If `false`, the caller should clean up the file by itself.
   */
  boolean registerTempFileToClean(DownloadFile file);
}
