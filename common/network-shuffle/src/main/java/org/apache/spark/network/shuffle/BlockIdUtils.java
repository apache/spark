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

import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;

public class BlockIdUtils {
    public static final String SHUFFLE_BLOCK_PREFIX = "shuffle_";

    public static final String SHUFFLE_CHUNK_PREFIX = "shuffleChunk_";

    public static final String RDD_BLOCK_PREFIX = "rdd_";

    private static final String NOOP_REDUCE_ID = "0";

    public static Boolean isRddBlock(String blockId) {
        if (blockId.startsWith(RDD_BLOCK_PREFIX)) return true;
        return false;
    }

    public static Boolean isShuffleBlock(String blockId) {
        if (blockId.startsWith(SHUFFLE_BLOCK_PREFIX)) return true;
        return false;
    }

    public static Boolean isShuffleChunk(String blockId) {
        // Shuffle chunk block is not supported by Aether, and we don't need to check for wrapped
        // Aether blocks.
        return blockId.startsWith(SHUFFLE_CHUNK_PREFIX);
    }

    public static int getShuffleId(String blockId) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length <  4) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockId);
        }
        return Integer.parseInt(blockIdParts[1]);
    }

    public static long getMapId(String blockId) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length <  4) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockId);
        }
        return Long.parseLong(blockIdParts[2]);
    }

    public static int getReduceId(String blockId) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length <  4) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockId);
        }
        return Integer.parseInt(blockIdParts[3]);
    }

    /**
     * Returns the name of the ShuffleChecksumBlockId. This should be in sync with
     *  IndexShuffleBlockResolver.getChecksumFile
     */
    static public String getChecksumFileName(String blockId, String algorithm) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length <  4) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockId);
        }
        // Replace the reduceId with NOOP_REDUCE_ID to get the checksum file name
        if (blockIdParts.length == 4 && isShuffleBlock(blockId)) {
          blockIdParts[3] = NOOP_REDUCE_ID;
        } else {
          throw new IllegalArgumentException("Unexpected block id format");
        }

        return ShuffleChecksumHelper.getChecksumFileName(
         String.join("_", blockIdParts) + ".checksum", algorithm);
    }
}
