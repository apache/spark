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

package org.apache.spark.shuffle.checksum;

import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;

public interface ShuffleChecksumSupport {

  default Checksum[] createPartitionChecksums(int numPartitions, SparkConf conf) {
    if ((boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED())) {
      String checksumAlgorithm = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
      return ShuffleChecksumHelper.createPartitionChecksums(numPartitions, checksumAlgorithm);
    } else {
      return ShuffleChecksumHelper.EMPTY_CHECKSUM;
    }
  }

  default long[] getChecksumValues(Checksum[] partitionChecksums) {
    int numPartitions = partitionChecksums.length;
    long[] checksumValues = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      checksumValues[i] = partitionChecksums[i].getValue();
    }
    return checksumValues;
  }
}
