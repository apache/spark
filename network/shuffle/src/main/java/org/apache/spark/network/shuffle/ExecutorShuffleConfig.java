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

import java.io.Serializable;

/** Contains all configuration necessary for a single Executor to find its shuffle files. */
public class ExecutorShuffleConfig implements Serializable {
  /** The base set of local directories that the executor stores its shuffle files in. */
  final String[] localDirs;
  /** Number of subdirectories created within each localDir. */
  final int subDirsPerLocalDir;
  /** Shuffle manager (SortShuffleManager or HashShuffleManager) that the executor is using. */
  final String shuffleManager;

  public ExecutorShuffleConfig(String[] localDirs, int subDirsPerLocalDir, String shuffleManager) {
    this.localDirs = localDirs;
    this.subDirsPerLocalDir = subDirsPerLocalDir;
    this.shuffleManager = shuffleManager;
  }
}
