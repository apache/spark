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

package org.apache.spark.remoteshuffle.util;

import com.google.common.net.InetAddresses;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class NetworkUtils {
  public static final int DEFAULT_REACHABLE_TIMEOUT = 30000;

  private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

  public static String getLocalHostName() {
    try {
      Map<String, String> env = System.getenv();
      if (env.containsKey("COMPUTERNAME"))
        return env.get("COMPUTERNAME");
      else if (env.containsKey("HOSTNAME"))
        return env.get("HOSTNAME");
      else
        return InetAddress.getLocalHost().getHostName();
    } catch (Throwable e) {
      return "localhost";
    }
  }

  // We use FQDN to register shuffle server because some data center needs FQDN to resolve the server to ip address.
  public static String getLocalFQDN() {
    InetAddress address;
    try {
      address = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RssNetworkException("Unable to fetch address details for localhost", e);
    }
    String result = address.getCanonicalHostName();
    if (result.toLowerCase().equals("localhost")) {
      result = address.getHostName();
    }
    return result;
  }

  public static boolean isReachable(String host, int timeout) {
    if (host == null || host.isEmpty()) {
      return false;
    }

    try {
      InetAddress inetAddress;
      if (InetAddresses.isInetAddress(host)) {
        inetAddress = InetAddresses.forString(host);
      } else {
        inetAddress = InetAddress.getByName(host);
      }
      return inetAddress.isReachable(timeout);
    } catch (IOException ex) {
      logger.warn(String
              .format("Host %s not reachable due to %s", host, ExceptionUtils.getSimpleMessage(ex)),
          ex);
      return false;
    }
  }
}
