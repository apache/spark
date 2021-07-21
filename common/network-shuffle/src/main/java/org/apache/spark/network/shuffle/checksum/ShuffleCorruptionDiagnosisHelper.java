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

package org.apache.spark.network.shuffle.checksum;

import java.io.*;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.corruption.Cause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utility functions for the shuffle checksum.
 */
@Private
public class ShuffleCorruptionDiagnosisHelper {
  private static final Logger logger =
    LoggerFactory.getLogger(ShuffleCorruptionDiagnosisHelper.class);

  public static final int CHECKSUM_CALCULATION_BUFFER = 8192;

  private static Checksum[] getChecksumByAlgorithm(int num, String algorithm)
    throws UnsupportedOperationException {
    Checksum[] checksums;
    switch (algorithm) {
      case "ADLER32":
        checksums = new Adler32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new Adler32();
        }
        return checksums;

      case "CRC32":
        checksums = new CRC32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new CRC32();
        }
        return checksums;

      default:
        throw new UnsupportedOperationException("Unsupported shuffle checksum algorithm: " +
          algorithm);
    }
  }

  public static Checksum getChecksumByFileExtension(String fileName)
    throws UnsupportedOperationException {
    int index = fileName.lastIndexOf(".");
    String algorithm = fileName.substring(index + 1);
    return getChecksumByAlgorithm(1, algorithm)[0];
  }

  private static long readChecksumByReduceId(File checksumFile, int reduceId) throws IOException {
    try (DataInputStream in = new DataInputStream(new FileInputStream(checksumFile))) {
      in.skip(reduceId * 8L);
      return in.readLong();
    }
  }

  private static long calculateChecksumForPartition(
      ManagedBuffer partitionData,
      Checksum checksumAlgo) throws IOException {
    InputStream in = partitionData.createInputStream();
    byte[] buffer = new byte[CHECKSUM_CALCULATION_BUFFER];
    try(CheckedInputStream checksumIn = new CheckedInputStream(in, checksumAlgo)) {
      while (checksumIn.read(buffer, 0, CHECKSUM_CALCULATION_BUFFER) != -1) {}
      return checksumAlgo.getValue();
    }
  }

  public static Cause diagnoseCorruption(
      File checksumFile,
      int reduceId,
      ManagedBuffer partitionData,
      long checksumByReader) {
    Cause cause;
    if (checksumFile.exists()) {
      try {
        long diagnoseStart = System.currentTimeMillis();
        long checksumByWriter = readChecksumByReduceId(checksumFile, reduceId);
        Checksum checksumAlgo = getChecksumByFileExtension(checksumFile.getName());
        long checksumByReCalculation = calculateChecksumForPartition(partitionData, checksumAlgo);
        long duration = System.currentTimeMillis() - diagnoseStart;
        logger.info("Shuffle corruption diagnosis took " + duration + " ms");
        if (checksumByWriter != checksumByReCalculation) {
          cause = Cause.DISK_ISSUE;
        } else if (checksumByWriter != checksumByReader) {
          cause = Cause.NETWORK_ISSUE;
        } else {
          cause = Cause.CHECKSUM_VERIFY_PASS;
        }
      } catch (Exception e) {
        logger.warn("Exception throws while diagnosing shuffle block corruption.", e);
        cause = Cause.UNKNOWN_ISSUE;
      }
    } else {
      // Even if checksum is enabled, a checksum file may not exist if error throws during writing.
      logger.warn("Checksum file " + checksumFile.getName() + " doesn't exit");
      cause = Cause.UNKNOWN_ISSUE;
    }
    return cause;
  }
}
