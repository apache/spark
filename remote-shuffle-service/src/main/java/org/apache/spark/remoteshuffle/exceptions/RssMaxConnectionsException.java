/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
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

package org.apache.spark.remoteshuffle.exceptions;

public class RssMaxConnectionsException extends Exception {
  private String message;
  private int currentConnections = -1;
  private int maxConnections = -1;

  public RssMaxConnectionsException(int currentConnections, int maxConnections, String message) {
    super(message);
    this.message = message;
    this.currentConnections = currentConnections;
    this.maxConnections = maxConnections;
  }

  public int getCurrentConnections() {
    return currentConnections;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  @Override
  public String getMessage() {
    return String.format("%s, current: %s, max: %s", this.message, this.currentConnections,
        this.maxConnections);
  }

  @Override
  public String toString() {
    return "RssMaxConnectionsException{" +
        "currentConnections=" + currentConnections +
        ", maxConnections=" + maxConnections +
        "} " + super.toString();
  }
}
