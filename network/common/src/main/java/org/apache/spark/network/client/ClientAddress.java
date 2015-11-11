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

package org.apache.spark.network.client;

import java.io.Serializable;
import java.net.SocketAddress;

/**
 * Just a simple immutable class which includes socket address along with app ID for which the
 * connection is authenticated.
 */
class ClientAddress implements Serializable {
  public final SocketAddress socketAddress;
  public final String appId;

  public static ClientAddress from(SocketAddress socketAddres, String appId) {
    if (socketAddres == null) {
      throw new NullPointerException("SocketAddress cannot be null");
    }
    return new ClientAddress(socketAddres, appId);
  }

  private ClientAddress(SocketAddress socketAddress, String appId) {
    this.socketAddress = socketAddress;
    this.appId = appId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClientAddress that = (ClientAddress) o;

    if (!socketAddress.equals(that.socketAddress)) return false;
    return !(appId != null ? !appId.equals(that.appId) : that.appId != null);

  }

  @Override
  public int hashCode() {
    int result = socketAddress.hashCode();
    result = 31 * result + (appId != null ? appId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ClientAddress{" +
      "socketAddress=" + socketAddress +
      ", appId='" + appId + '\'' +
      '}';
  }
}
