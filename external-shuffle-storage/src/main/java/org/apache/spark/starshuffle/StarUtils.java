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

import org.apache.spark.SparkConf;
import org.apache.spark.storage.FileSegment;

public class StarUtils {

    public static long[] getLengths(FileSegment[] fileSegments) {
        long[] lengths = new long[fileSegments.length];
        for (int i = 0; i < lengths.length; i++) {
            lengths[i] = fileSegments[i].length();
        }
        return lengths;
    }

    public static StarShuffleFileManager createShuffleFileManager(SparkConf conf, String path) {
        if (path == null || path.isEmpty() || path.startsWith("/")) {
            return new StarLocalFileShuffleFileManager();
        } else if (path.toLowerCase().startsWith("s3")) {
            return new StarS3ShuffleFileManager(conf);
        } else {
            throw new RuntimeException(String.format(
                    "Unsupported path for StarShuffleFileManager: %s", path));
        }
    }
}
