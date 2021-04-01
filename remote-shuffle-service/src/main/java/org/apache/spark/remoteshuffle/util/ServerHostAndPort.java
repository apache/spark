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

package org.apache.spark.remoteshuffle.util;

import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;

import java.util.Objects;

public class ServerHostAndPort {
  private final String host;
  private final int port;

  public static ServerHostAndPort fromString(String str) {
    return new ServerHostAndPort(str);
  }

  public ServerHostAndPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public ServerHostAndPort(String str) {
    if (str == null) {
      this.host = null;
      this.port = 0;
    } else {
      String[] strArray = str.split(":");
      if (strArray.length == 1) {
        this.host = strArray[0];
        this.port = 0;
      } else if (strArray.length == 2) {
        this.host = strArray[0];
        try {
          this.port = Integer.parseInt(strArray[1].trim());
        } catch (Throwable ex) {
          throw new RssInvalidDataException(
              String.format("Invalid host and port string: %s", str));
        }
      } else {
        throw new RssInvalidDataException(String.format("Invalid host and port string: %s", str));
      }
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerHostAndPort that = (ServerHostAndPort) o;
    return port == that.port &&
        Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", host, port);
  }
}
