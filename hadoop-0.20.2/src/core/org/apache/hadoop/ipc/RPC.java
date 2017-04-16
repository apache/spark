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

package org.apache.hadoop.ipc;

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.io.*;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import org.apache.hadoop.net.NetUtils;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  private static final Log LOG =
    LogFactory.getLog(RPC.class);

  private RPC() {}                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = UTF8.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, Client> clients =
      new HashMap<SocketFactory, Client>();

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory 
     * if no cached client exists.
     * 
     * @param conf Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      Client client = clients.get(factory);
      if (client == null) {
        client = new Client(ObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory 
     * if no cached client exists.
     * 
     * @param conf Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection 
     * A RPC client is closed only when its reference count becomes zero.
     */
    private void stopClient(Client client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  private static ClientCache CLIENTS=new ClientCache();
  
  //for unit testing only
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf);
  }
  
  private static class Invoker implements InvocationHandler {
    private Client.ConnectionId remoteId;
    private Client client;
    private boolean isClosed = false;

    public Invoker(Class<? extends VersionedProtocol> protocol,
        InetSocketAddress address, UserGroupInformation ticket,
        Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      final boolean logDebug = LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }

      ObjectWritable value = (ObjectWritable)
        client.call(new Invocation(method, args), remoteId);
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    synchronized private void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }
  
  public static VersionedProtocol waitForProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProxy(protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  static VersionedProtocol waitForProxy(
                      Class<? extends VersionedProtocol> protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               long connTimeout
                                               ) throws IOException {
    return waitForProxy(protocol, clientVersion, addr, conf, 0, connTimeout);
  }
  
  /**
   * Get a proxy connection to a remote server
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  static VersionedProtocol waitForProxy(
                      Class<? extends VersionedProtocol> protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               int rpcTimeout,
                                               long connTimeout
                                               ) throws IOException {
      long startTime = System.currentTimeMillis();
    IOException ioe;
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr,
            UserGroupInformation.getCurrentUser(), conf, NetUtils
            .getDefaultSocketFactory(conf), rpcTimeout);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch(NoRouteToHostException nrthe) { // perhaps a VIP is failing over
        LOG.info("No route to host for server: " + addr);
        ioe = nrthe;
      }
      // check if timed out
      if (System.currentTimeMillis()-connTimeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory, 0);
  }
  
  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(protocol, addr, ticket, conf, factory, rpcTimeout));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(), 
                                                  clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion, 
                                serverVersion);
    }
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, 
        NetUtils.getDefaultSocketFactory(conf));
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param proxy the proxy to be stopped
   */
  public static void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  /** 
   * Expert: Make multiple, parallel calls to a set of servers.
   * @deprecated Use {@link #call(Method, Object[][], InetSocketAddress[], UserGroupInformation, Configuration)} instead 
   */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException, InterruptedException {
    return call(method, params, addrs, null, conf);
  }
  
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, 
                              UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    Client client = CLIENTS.getClient(conf);
    try {
    Writable[] wrappedValues = 
      client.call(invocations, addrs, method.getDeclaringClass(), ticket, conf);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, numHandlers, verbose, conf, null);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address, with a secret manager. */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf,
                                 SecretManager<? extends TokenIdentifier> secretManager) 
    throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose, secretManager);
  }
  
  /** An RPC Server. */
  public static class Server extends org.apache.hadoop.ipc.Server {
    private Object instance;
    private boolean verbose;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false, null);
    }
    
    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }
    
    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose, 
                  SecretManager<? extends TokenIdentifier> secretManager) 
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf,
          classNameBase(instance.getClass().getName()), secretManager);
      this.instance = instance;
      this.verbose = verbose;
    }

    public Writable call(Class<?> protocol, Writable param, long receivedTime) 
    throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);

        Method method =
          protocol.getMethod(call.getMethodName(),
                                   call.getParameterClasses());
        method.setAccessible(true);

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() +
                    " queueTime= " + qTime +
                    " procesingTime= " + processingTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);

        MetricsTimeVaryingRate m =
         (MetricsTimeVaryingRate) rpcDetailedMetrics.registry.get(call.getMethodName());
      	if (m == null) {
      	  try {
      	    m = new MetricsTimeVaryingRate(call.getMethodName(),
      	                                        rpcDetailedMetrics.registry);
      	  } catch (IllegalArgumentException iae) {
      	    // the metrics has been registered; re-fetch the handle
      	    LOG.info("Error register " + call.getMethodName(), iae);
      	    m = (MetricsTimeVaryingRate) rpcDetailedMetrics.registry.get(
      	        call.getMethodName());
      	  }
      	}
        m.inc(processingTime);

        if (verbose) log("Return: "+value);

        return new ObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  private static void log(String value) {
    if (value!= null && value.length() > 55)
      value = value.substring(0, 55)+"...";
    LOG.info(value);
  }
}
