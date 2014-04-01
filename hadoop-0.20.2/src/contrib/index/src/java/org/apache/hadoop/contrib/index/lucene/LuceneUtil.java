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

package org.apache.hadoop.contrib.index.lucene;

import java.io.IOException;

import org.apache.lucene.store.Directory;

/**
 * This class copies some methods from Lucene's SegmentInfos since that class
 * is not public.
 */
public final class LuceneUtil {

  static final class IndexFileNames {
    /** Name of the index segment file */
    static final String SEGMENTS = "segments";

    /** Name of the generation reference file name */
    static final String SEGMENTS_GEN = "segments.gen";
  }

  /**
   * Check if the file is a segments_N file
   * @param name
   * @return true if the file is a segments_N file
   */
  public static boolean isSegmentsFile(String name) {
    return name.startsWith(IndexFileNames.SEGMENTS)
        && !name.equals(IndexFileNames.SEGMENTS_GEN);
  }

  /**
   * Check if the file is the segments.gen file
   * @param name
   * @return true if the file is the segments.gen file
   */
  public static boolean isSegmentsGenFile(String name) {
    return name.equals(IndexFileNames.SEGMENTS_GEN);
  }

  /**
   * Get the generation (N) of the current segments_N file in the directory.
   * 
   * @param directory -- directory to search for the latest segments_N file
   */
  public static long getCurrentSegmentGeneration(Directory directory)
      throws IOException {
    String[] files = directory.list();
    if (files == null)
      throw new IOException("cannot read directory " + directory
          + ": list() returned null");
    return getCurrentSegmentGeneration(files);
  }

  /**
   * Get the generation (N) of the current segments_N file from a list of
   * files.
   * 
   * @param files -- array of file names to check
   */
  public static long getCurrentSegmentGeneration(String[] files) {
    if (files == null) {
      return -1;
    }
    long max = -1;
    for (int i = 0; i < files.length; i++) {
      String file = files[i];
      if (file.startsWith(IndexFileNames.SEGMENTS)
          && !file.equals(IndexFileNames.SEGMENTS_GEN)) {
        long gen = generationFromSegmentsFileName(file);
        if (gen > max) {
          max = gen;
        }
      }
    }
    return max;
  }

  /**
   * Parse the generation off the segments file name and return it.
   */
  public static long generationFromSegmentsFileName(String fileName) {
    if (fileName.equals(IndexFileNames.SEGMENTS)) {
      return 0;
    } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
      return Long.parseLong(
          fileName.substring(1 + IndexFileNames.SEGMENTS.length()),
          Character.MAX_RADIX);
    } else {
      throw new IllegalArgumentException("fileName \"" + fileName
          + "\" is not a segments file");
    }
  }

}
