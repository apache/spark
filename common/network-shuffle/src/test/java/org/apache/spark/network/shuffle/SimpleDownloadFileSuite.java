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

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Assertions;

public class SimpleDownloadFileSuite {
  @Test
  public void testChannelIsClosedAfterCloseAndRead() throws IOException {
    File tempFile = File.createTempFile("testChannelIsClosed", ".tmp");
    tempFile.deleteOnExit();
    TransportConf conf = new TransportConf("test", MapConfigProvider.EMPTY);

    DownloadFile downloadFile = null;
    try {
      downloadFile = new SimpleDownloadFile(tempFile, conf);
      DownloadFileWritableChannel channel = downloadFile.openForWriting();
      channel.closeAndRead();
      Assertions.assertFalse(channel.isOpen(), "Channel should be closed after closeAndRead.");
    } finally {
      if (downloadFile != null) {
        downloadFile.delete();
      }
    }
  }
}
