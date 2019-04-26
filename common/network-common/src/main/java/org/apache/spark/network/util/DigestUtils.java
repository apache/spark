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

package org.apache.spark.network.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

public class DigestUtils {
    private static final int STREAM_BUFFER_LENGTH = 8192;
    private static final int DIGEST_LENGTH = 8;

    public static int getDigestLength() {
       return DIGEST_LENGTH;
    }

    public static long getDigest(InputStream data) throws IOException {
        return updateCRC32(getCRC32(), data);
    }

    public static long getDigest(File file, long offset, long length) {
        try {
            LimitedInputStream inputStream = new LimitedInputStream(new FileInputStream(file),
              offset + length, true);
            inputStream.skip(offset);
            return getDigest(inputStream);
        } catch (IOException e) {
            return -1;
        }
    }

    public static CRC32 getCRC32() {
        return new CRC32();
    }

    public static long updateCRC32(CRC32 crc32, InputStream data) throws IOException {
        byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
        int len;
        while ((len = data.read(buffer)) >= 0) {
            crc32.update(buffer, 0, len);
        }
        return crc32.getValue();
    }
}
