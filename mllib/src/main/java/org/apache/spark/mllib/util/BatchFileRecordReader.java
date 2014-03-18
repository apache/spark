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

package org.apache.spark.mllib.util;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads an entire file out in bytes format in <filename, content> format.
 */

public class BatchFileRecordReader extends RecordReader<String, Text> {
    private long startOffset;
    private long end;
    private long pos;
    private Path path;

    private static final int MAX_BYTES_ALLOCATION = 64 * 1024 * 1024;

    private String key = null;
    private Text value = null;

    private FSDataInputStream fileIn;

    public BatchFileRecordReader(
            CombineFileSplit split,
            TaskAttemptContext context,
            Integer index)
            throws IOException {
        path = split.getPath(index);
        startOffset = split.getOffset(index);
        pos = startOffset;
        end = startOffset + split.getLength(index);

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        fileIn = fs.open(path);
        fileIn.seek(startOffset);
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {}

    @Override
    public void close() throws IOException {
        if (fileIn != null) {
            fileIn.close();
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (startOffset == end) return 0;
        return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException{
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = path.getName();
        }
        if (value == null) {
            value = new Text();
        }

        if (pos >= end) {
            return false;
        }

        int maxBufferLength = end - pos < Integer.MAX_VALUE ? (int) (end - pos) : Integer.MAX_VALUE;
        if (maxBufferLength > MAX_BYTES_ALLOCATION) {
            maxBufferLength = MAX_BYTES_ALLOCATION;
        }

        byte[] innerBuffer = new byte[maxBufferLength];

        int len = fileIn.read(pos, innerBuffer, 0, maxBufferLength);
        pos += len;

        value.set(innerBuffer, 0, len);

        return true;
    }
}
