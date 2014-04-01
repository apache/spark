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

package org.apache.hadoop.contrib.index.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A class implements an index updater interface should create a Map/Reduce job
 * configuration and run the Map/Reduce job to analyze documents and update
 * Lucene instances in parallel.
 */
public interface IIndexUpdater {

  /**
   * Create a Map/Reduce job configuration and run the Map/Reduce job to
   * analyze documents and update Lucene instances in parallel.
   * @param conf
   * @param inputPaths
   * @param outputPath
   * @param numMapTasks
   * @param shards
   * @throws IOException
   */
  void run(Configuration conf, Path[] inputPaths, Path outputPath,
      int numMapTasks, Shard[] shards) throws IOException;

}
