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

import org.apache.commons.io.IOUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * This class read/write shuffle file on external storage like local file or network file system.
 */
public class StarLocalFileShuffleFileManager implements StarShuffleFileManager {
    private static final Logger logger = LoggerFactory.getLogger(
            StarLocalFileShuffleFileManager.class);

    @Override
    public String createFile(String root) {
        try {
            if (root == null || root.isEmpty()) {
                File file = File.createTempFile("shuffle", ".data");
                return file.getAbsolutePath();
            } else {
                String fileName = String.format("shuffle-%s.data", UUID.randomUUID());
                return Paths.get(root, fileName).toString();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create shuffle file", e);
        }
    }

    @Override
    public void write(InputStream data, long size, String file) {
        logger.info("Writing to shuffle file: {}", file);
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            long copiedBytes = IOUtils.copyLarge(data, outputStream);
            if (copiedBytes != size) {
                throw new RuntimeException(String.format(
                        "Got corrupted shuffle data when writing to " +
                                "file %s, expected size: %s, actual written size: %s",
                        file, copiedBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to write shuffle file %s",
                    file));
        }
    }

    @Override
    public InputStream read(String file, long offset, long size) {
        logger.info("Opening shuffle file: {}, offset: {}, size: {}", file, offset, size);
        try {
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.skip(offset);
            return new LimitedInputStream(inputStream, size);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to open shuffle file %s, offset: %s, size: %s",
                    file, offset, size));
        }
    }
}
