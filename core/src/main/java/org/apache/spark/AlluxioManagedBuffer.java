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

package org.apache.spark;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.storage.ExternalBlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.toIntExact;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 */
public final class AlluxioManagedBuffer extends ManagedBuffer {
    private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConf conf;
    private final Path file;
    private final long offset;
    private final long length;
    private final ExternalBlockManager blockManager;

    public AlluxioManagedBuffer(TransportConf conf, Path file, long offset, long length, ExternalBlockManager blockManager) {
        this.conf = conf;
        this.file = file;
        this.offset = offset;
        this.length = length;
        this.blockManager = blockManager;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        FileSystem fs = blockManager.fs();
        if (!fs.exists(file)) {
            return ByteBuffer.wrap(new byte[0]);
        } else {
            long size = fs.getFileStatus(file).getLen();
            if (size == 0) {
                return ByteBuffer.wrap(new byte[0]);
            } else {
                InputStream input = fs.open(file);
                try {
                    byte[] buffer = new byte[toIntExact(size)];
                    ByteStreams.readFully(input, buffer);
                    return ByteBuffer.wrap(buffer,toIntExact(offset),toIntExact(length));
                } catch (IOException e){
                    logger.info("Test-log: Failed to get bytes of block $blockId from Alluxio", e);
                    return ByteBuffer.wrap(new byte[0]);
                } finally {
                    input.close();
                }
            }
        }
    }

    @Override
    public InputStream createInputStream() throws IOException {

        logger.info("Test-log: use function createInputStream");
        InputStream is = blockManager.createInputStream(file).get();
        logger.info("Test-log: create inputStream successfully");
        try {

            ByteStreams.skipFully(is, offset);
            logger.info("Test-log: read data from inputstream from offset " + offset + " and size is " + length );
            return new LimitedInputStream(is, length);
        } catch (IOException e) {
            try {
                if (is != null) {
                    long size = blockManager.fs().getFileStatus(file).getLen();
                    throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                            e);
                }
            } catch (IOException ignored) {
                // ignore
            } finally {
                JavaUtils.closeQuietly(is);
            }
            throw new IOException("Error in opening " + this, e);
        } catch (RuntimeException e) {
            JavaUtils.closeQuietly(is);
            throw e;
        }
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return new Object();
    }

    public Path getFile() { return file; }

    public long getOffset() { return offset; }

    public long getLength() { return length; }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("file", file)
                .add("offset", offset)
                .add("length", length)
                .toString();
    }
}
