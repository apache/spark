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

package org.apache.spark.starshuffle;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkEnv;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.StarShuffleUtils;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.FileSegment;

/**
 * This class writes shuffle segments into the result file.
 */
public class StartFileSegmentWriter {
    private String rootDir;

    public StartFileSegmentWriter(String rootDir) {
        this.rootDir = rootDir;
    }

    public MapStatus write(long mapId, FileSegment[] fileSegments) {
        List<InputStream> allStreams = Arrays.stream(fileSegments).map(t -> {
            try {
                FileInputStream fileStream = new FileInputStream(t.file());
                fileStream.skip(t.offset());
                return new LimitedInputStream(fileStream, t.length());
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "Failed to read shuffle temp file %s", t.file()),
                        e);
            }
        }).collect(Collectors.toList());
        try (SequenceInputStream sequenceInputStream = new SequenceInputStream(
                Collections.enumeration(allStreams))) {
            StarShuffleFileManager shuffleFileManager = StarUtils.createShuffleFileManager(
                    SparkEnv.get().conf(), rootDir);
            String resultFile = shuffleFileManager.createFile(rootDir);
            long size = Arrays.stream(fileSegments).mapToLong(t->t.length()).sum();
            shuffleFileManager.write(sequenceInputStream, size, resultFile);
            long[] partitionLengths = new long[fileSegments.length];
            for (int i = 0; i < partitionLengths.length; i++) {
                partitionLengths[i] = fileSegments[i].length();
            }
            BlockManagerId blockManagerId = createMapTaskDummyBlockManagerId(
                    partitionLengths, resultFile);
            MapStatus mapStatus = MapStatus$.MODULE$.apply(
                    blockManagerId, partitionLengths, mapId);
            return mapStatus;
        } catch (IOException e) {
            throw new RuntimeException("Failed to close shuffle temp files", e);
        }
    }

    private BlockManagerId createMapTaskDummyBlockManagerId(long[] partitionLengths, String file) {
        return StarShuffleUtils.createDummyBlockManagerId(file, partitionLengths);
    }
}
