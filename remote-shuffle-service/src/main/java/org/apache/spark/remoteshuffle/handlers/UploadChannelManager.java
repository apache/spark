/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.handlers;

import org.apache.spark.remoteshuffle.exceptions.RssMaxConnectionsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class UploadChannelManager {
  public static final int DEFAULT_MAX_CONNECTIONS = 60000;

  private static final Logger logger = LoggerFactory.getLogger(UploadChannelManager.class);

  private AtomicInteger numConnections = new AtomicInteger();

  private int maxConnections = DEFAULT_MAX_CONNECTIONS;

  public UploadChannelManager() {
  }

  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public void checkMaxConnections() throws RssMaxConnectionsException {
    if (numConnections.get() > maxConnections) {
      throw new RssMaxConnectionsException(numConnections.get(), maxConnections,
          "Stream server connections exceed upper limit");
    }
  }

  public void incNumConnections() {
    numConnections.incrementAndGet();
  }

  public void decNumConnections() {
    numConnections.decrementAndGet();
  }
}
