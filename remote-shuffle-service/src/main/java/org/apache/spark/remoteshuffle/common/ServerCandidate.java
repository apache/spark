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

package org.apache.spark.remoteshuffle.common;

public class ServerCandidate {
  private ServerDetail serverDetail;
  private long requestLatency;
  private long concurrentConnections;

  public ServerCandidate(ServerDetail serverDetail, long requestLatency,
                         long concurrentConnections) {
    this.serverDetail = serverDetail;
    this.requestLatency = requestLatency;
    this.concurrentConnections = concurrentConnections;
  }

  public ServerDetail getServerDetail() {
    return serverDetail;
  }

  public long getRequestLatency() {
    return requestLatency;
  }

  public long getConcurrentConnections() {
    return concurrentConnections;
  }

  @Override
  public String toString() {
    return "ServerCandidate{" +
        "serverDetail=" + serverDetail +
        ", requestLatency=" + requestLatency +
        ", concurrentConnections=" + concurrentConnections +
        '}';
  }
}
