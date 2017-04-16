/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;
import java.util.*;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.thirdparty.guava.common.base.Preconditions;

public class NetUtils {
  private static final Log LOG = LogFactory.getLog(NetUtils.class);
  
  private static Map<String, String> hostToResolved = 
                                     new HashMap<String, String>();

  /**
   * Get the socket factory for the given class according to its
   * configuration parameter
   * <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>. When no
   * such parameter exists then fall back on the default socket factory as
   * configured by <tt>hadoop.rpc.socket.factory.class.default</tt>. If
   * this default socket factory is not configured, then fall back on the JVM
   * default socket factory.
   * 
   * @param conf the configuration
   * @param clazz the class (usually a {@link VersionedProtocol})
   * @return a socket factory
   */
  public static SocketFactory getSocketFactory(Configuration conf,
      Class<?> clazz) {

    SocketFactory factory = null;

    String propValue =
        conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
    if ((propValue != null) && (propValue.length() > 0))
      factory = getSocketFactoryFromProperty(conf, propValue);

    if (factory == null)
      factory = getDefaultSocketFactory(conf);

    return factory;
  }

  /**
   * Get the default socket factory as specified by the configuration
   * parameter <tt>hadoop.rpc.socket.factory.default</tt>
   * 
   * @param conf the configuration
   * @return the default socket factory as specified in the configuration or
   *         the JVM default socket factory if the configuration does not
   *         contain a default socket factory property.
   */
  public static SocketFactory getDefaultSocketFactory(Configuration conf) {

    String propValue = conf.get("hadoop.rpc.socket.factory.class.default");
    if ((propValue == null) || (propValue.length() == 0))
      return SocketFactory.getDefault();

    return getSocketFactoryFromProperty(conf, propValue);
  }

  /**
   * Get the socket factory corresponding to the given proxy URI. If the
   * given proxy URI corresponds to an absence of configuration parameter,
   * returns null. If the URI is malformed raises an exception.
   * 
   * @param propValue the property which is the class name of the
   *        SocketFactory to instantiate; assumed non null and non empty.
   * @return a socket factory as defined in the property value.
   */
  public static SocketFactory getSocketFactoryFromProperty(
      Configuration conf, String propValue) {

    try {
      Class<?> theClass = conf.getClassByName(propValue);
      return (SocketFactory) ReflectionUtils.newInstance(theClass, conf);

    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Socket Factory class not found: " + cnfe);
    }
  }

  /**
   * Util method to build socket addr from either:
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target,
                                                   int defaultPort) {
    if (target == null) {
      throw new IllegalArgumentException("Target address cannot be null.");
    }
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0 && defaultPort == -1) {
      throw new RuntimeException("Not a host:port pair: " + target);
    }
    String hostname;
    int port = -1;
    if (!target.contains("/")) {
      if (colonIndex == -1) {
        hostname = target;
      } else {
        // must be the old style <host>:<port>
        hostname = target.substring(0, colonIndex);
        port = Integer.parseInt(target.substring(colonIndex + 1));
      }
    } else {
      // a new uri
      URI addr = new Path(target).toUri();
      hostname = addr.getHost();
      port = addr.getPort();
    }

    if (port == -1) {
      port = defaultPort;
    }
  
    if (getStaticResolution(hostname) != null) {
      hostname = getStaticResolution(hostname);
    }
    return new InetSocketAddress(hostname, port);
  }

  /**
   * Handle the transition from pairs of attributes specifying a host and port
   * to a single colon separated one.
   * @param conf the configuration to check
   * @param oldBindAddressName the old address attribute name
   * @param oldPortName the old port attribute name
   * @param newBindAddressName the new combined name
   * @return the complete address from the configuration
   */
  @Deprecated
  public static String getServerAddress(Configuration conf,
                                        String oldBindAddressName,
                                        String oldPortName,
                                        String newBindAddressName) {
    String oldAddr = conf.get(oldBindAddressName);
    String oldPort = conf.get(oldPortName);
    String newAddrPort = conf.get(newBindAddressName);
    if (oldAddr == null && oldPort == null) {
      return newAddrPort;
    }
    String[] newAddrPortParts = newAddrPort.split(":",2);
    if (newAddrPortParts.length != 2) {
      throw new IllegalArgumentException("Invalid address/port: " + 
                                         newAddrPort);
    }
    if (oldAddr == null) {
      oldAddr = newAddrPortParts[0];
    } else {
      LOG.warn("Configuration parameter " + oldBindAddressName +
               " is deprecated. Use " + newBindAddressName + " instead.");
    }
    if (oldPort == null) {
      oldPort = newAddrPortParts[1];
    } else {
      LOG.warn("Configuration parameter " + oldPortName +
               " is deprecated. Use " + newBindAddressName + " instead.");      
    }
    return oldAddr + ":" + oldPort;
  }
  
  /**
   * Adds a static resolution for host. This can be used for setting up
   * hostnames with names that are fake to point to a well known host. For e.g.
   * in some testcases we require to have daemons with different hostnames
   * running on the same machine. In order to create connections to these
   * daemons, one can set up mappings from those hostnames to "localhost".
   * {@link NetUtils#getStaticResolution(String)} can be used to query for
   * the actual hostname. 
   * @param host
   * @param resolvedName
   */
  public static void addStaticResolution(String host, String resolvedName) {
    synchronized (hostToResolved) {
      hostToResolved.put(host, resolvedName);
    }
  }
  
  /**
   * Retrieves the resolved name for the passed host. The resolved name must
   * have been set earlier using 
   * {@link NetUtils#addStaticResolution(String, String)}
   * @param host
   * @return the resolution
   */
  public static String getStaticResolution(String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }
  
  /**
   * This is used to get all the resolutions that were added using
   * {@link NetUtils#addStaticResolution(String, String)}. The return
   * value is a List each element of which contains an array of String 
   * of the form String[0]=hostname, String[1]=resolved-hostname
   * @return the list of resolutions
   */
  public static List <String[]> getAllStaticResolutions() {
    synchronized (hostToResolved) {
      Set <Entry <String, String>>entries = hostToResolved.entrySet();
      if (entries.size() == 0) {
        return null;
      }
      List <String[]> l = new ArrayList<String[]>(entries.size());
      for (Entry<String, String> e : entries) {
        l.add(new String[] {e.getKey(), e.getValue()});
      }
    return l;
    }
  }
  
  /**
   * Returns InetSocketAddress that a client can use to 
   * connect to the server. Server.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "127.0.0.1:port" when
   * the getListenerAddress() returns "0.0.0.0:port".
   * 
   * @param server
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(Server server) {
    InetSocketAddress addr = server.getListenerAddress();
    if (addr.getAddress().getHostAddress().equals("0.0.0.0")) {
      addr = new InetSocketAddress("127.0.0.1", addr.getPort());
    }
    return addr;
  }
  
  /**
   * Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
   * <br><br>
   * 
   * @see #getInputStream(Socket, long)
   */
  public static InputStream getInputStream(Socket socket) 
                                           throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }
  
  /**
   * Return a {@link SocketInputWrapper} for the socket and set the given
   * timeout. If the socket does not have an associated channel, then its socket
   * timeout will be set to the specified value. Otherwise, a
   * {@link SocketInputStream} will be created which reads with the configured
   * timeout.
   * 
   * Any socket created using socket factories returned by {@link #NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   *
   * In general, this should be called only once on each socket: see the note
   * in {@link SocketInputWrapper#setTimeout(long)} for more information.
   *     
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. zero for waiting as
   *                long as necessary.
   * @return SocketInputWrapper for reading from the socket.
   * @throws IOException
   */
  public static SocketInputWrapper getInputStream(Socket socket, long timeout) 
                                           throws IOException {
    InputStream stm = (socket.getChannel() == null) ? 
          socket.getInputStream() : new SocketInputStream(socket);
    SocketInputWrapper w = new SocketInputWrapper(socket, stm);
    w.setTimeout(timeout);
    return w;
  }
  
  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will
   * wait until data is available.<br><br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link #NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */  
  public static OutputStream getOutputStream(Socket socket) 
                                             throws IOException {
    return getOutputStream(socket, 0);
  }
  
  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link #NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException   
   */
  public static OutputStream getOutputStream(Socket socket, long timeout) 
                                             throws IOException {
    return (socket.getChannel() == null) ? 
            socket.getOutputStream() : new SocketOutputStream(socket, timeout);            
  }

  /**
   * This is a drop-in replacement for 
   * {@link Socket#connect(SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this 
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If 
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local 
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket,
      SocketAddress address,
      int timeout) throws IOException {
    connect(socket, address, null, timeout);
  }

  /**
   * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
   * also takes a local address and port to bind the socket to. 
   * 
   * @param socket
   * @param address the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket, 
                             SocketAddress endpoint,
                             SocketAddress localAddr,
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }
    
    SocketChannel ch = socket.getChannel();
    
    if (localAddr != null) {
      Class localClass = localAddr.getClass();
      Class remoteClass = endpoint.getClass();
      Preconditions.checkArgument(localClass.equals(remoteClass),
          "Local address %s must be of same family as remote address %s.",
          localAddr, endpoint);
      socket.bind(localAddr);
    }

    if (ch == null) {
      // let the default implementation handle it.
      socket.connect(endpoint, timeout);
    } else {
      SocketIOWithTimeout.connect(ch, endpoint, timeout);
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new ConnectException(
        "Localhost targeted connection resulted in a loopback. " +
        "No daemon is listening on the target port.");
    }
  }
  
  /** 
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   * 
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  public static String normalizeHostName(String name) {
    if (Character.digit(name.charAt(0), 10) != -1) { //FIXME 
      return name;
    } else {
      try {
        InetAddress ipAddress = InetAddress.getByName(name);
        return ipAddress.getHostAddress();
      } catch (UnknownHostException e) {
        return name;
      }
    }
  }
  
  /** 
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   * 
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  public static List<String> normalizeHostNames(Collection<String> names) {
    List<String> hostNames = new ArrayList<String>(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  /**
   * @return true if the given string is a subnet specified
   *     using CIDR notation, false otherwise
   */
  public static boolean isValidSubnet(String subnet) {
    try {
      new SubnetUtils(subnet);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }

  /**
   * Add all addresses associated with the given nif in the
   * given subnet to the given list.
   */
  private static void addMatchingAddrs(NetworkInterface nif,
      SubnetInfo subnetInfo, List<InetAddress> addrs) {
    Enumeration<InetAddress> ifAddrs = nif.getInetAddresses();
    while (ifAddrs.hasMoreElements()) {
      InetAddress ifAddr = ifAddrs.nextElement();
      if (subnetInfo.isInRange(ifAddr.getHostAddress())) {
        addrs.add(ifAddr);
      }
    }
  }

  /**
   * Return an InetAddress for each interface that matches the
   * given subnet specified using CIDR notation.
   *
   * @param subnet subnet specified using CIDR notation
   * @param returnSubinterfaces
   *            whether to return IPs associated with subinterfaces
   * @throws IllegalArgumentException if subnet is invalid
   */
  public static List<InetAddress> getIPs(String subnet,
      boolean returnSubinterfaces) {
    List<InetAddress> addrs = new ArrayList<InetAddress>();
    SubnetInfo subnetInfo = new SubnetUtils(subnet).getInfo();
    Enumeration<NetworkInterface> nifs;

    try {
      nifs = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOG.error("Unable to get host interfaces", e);
      return addrs;
    }

    while (nifs.hasMoreElements()) {
      NetworkInterface nif = nifs.nextElement();
      // NB: adding addresses even if the nif is not up
      addMatchingAddrs(nif, subnetInfo, addrs);

      if (!returnSubinterfaces) {
        continue;
      }
      Enumeration<NetworkInterface> subNifs = nif.getSubInterfaces();
      while (subNifs.hasMoreElements()) {
        addMatchingAddrs(subNifs.nextElement(), subnetInfo, addrs);
      }
    }
    return addrs;
  }
}
