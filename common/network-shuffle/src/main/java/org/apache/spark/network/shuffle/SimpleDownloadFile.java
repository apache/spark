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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.TransportConf;

/**
 * A DownloadFile that does not take any encryption settings into account for reading and
 * writing data.
 *
 * This does *not* mean the data in the file is un-encrypted -- it could be that the data is
 * already encrypted when its written, and subsequent layer is responsible for decrypting.
 */
public class SimpleDownloadFile implements DownloadFile {

  private final File file;
  private final TransportConf transportConf;

  public SimpleDownloadFile(File file, TransportConf transportConf) {
    this.file = file;
    this.transportConf = transportConf;
  }

  @Override
  public boolean delete() {
    return file.delete();
  }

  @Override
  public DownloadFileWritableChannel openForWriting() throws IOException {
    return new SimpleDownloadWritableChannel();
  }

  @Override
  public String path() {
    return file.getAbsolutePath();
  }

  private class SimpleDownloadWritableChannel implements DownloadFileWritableChannel {

    private final WritableByteChannel channel;

    SimpleDownloadWritableChannel() throws FileNotFoundException {
      channel = Channels.newChannel(new FileOutputStream(file));
    }

    @Override
    public ManagedBuffer closeAndRead() {
      return new FileSegmentManagedBuffer(transportConf, file, 0, file.length());
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      return channel.write(src);
    }

    @Override
    public boolean isOpen() {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
