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

package org.apache.spark.shuffle.sort.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;
import org.apache.spark.util.Utils;

public class LocalDiskSingleSpillMapOutputWriter
    implements SingleSpillShuffleMapOutputWriter {

  private final int shuffleId;
  private final long mapId;
  private final IndexShuffleBlockResolver blockResolver;

  public LocalDiskSingleSpillMapOutputWriter(
      int shuffleId,
      long mapId,
      IndexShuffleBlockResolver blockResolver) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
  }

  @Override
  public MapOutputCommitMessage transferMapSpillFile(
      File mapSpillFile, long[] partitionLengths) throws IOException {
    // The map spill file already has the proper format, and it contains all of the partition data.
    // So just transfer it directly to the destination without any merging.
    File outputFile = blockResolver.getDataFile(shuffleId, mapId);
    File tempFile = Utils.tempFileWith(outputFile);
    Files.move(mapSpillFile.toPath(), tempFile.toPath());
    blockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tempFile);
    return MapOutputCommitMessage.of(partitionLengths);
  }
}
