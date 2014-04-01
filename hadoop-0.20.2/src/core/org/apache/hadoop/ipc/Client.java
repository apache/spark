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

import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.ConnectException;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.security.PrivilegedExceptionAction;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.util.ReflectionUtils;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
public class Client {
  
  public static final Log LOG =
    LogFactory.getLog(Client.class);
  private Hashtable<ConnectionId, Connection> connections =
    new Hashtable<ConnectionId, Connection>();

  private Class<? extends Writable> valueClass;   // class of call values
  private int counter;                            // counter for call ids
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf;

  private SocketFactory socketFactory;           // how to create sockets
  private int refCount = 1;
  
  final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
  final static int DEFAULT_PING_INTERVAL = 60000; // 1 min
  final static int PING_CALL_ID = -1;
  
  private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactory() {
    private final ThreadFactory defaultThreadFactory = 
      Executors.defaultThreadFactory();
    private final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = defaultThreadFactory.newThread(r);
        
      thread.setDaemon(true);
      thread.setName("sendParams-" + counter.getAndIncrement());
        
      return thread;
    }
  };
  
  private static final ExecutorService SEND_PARAMS_EXECUTOR = 
    Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);
  
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(Configuration conf, int pingInterval) {
    conf.setInt(PING_INTERVAL_NAME, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
  }
  
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  /** A call waiting for a value. */
  private class Call {
    int id;                                       // call id
    Writable param;                               // parameter
    Writable value;                               // value, null if error
    IOException error;                            // exception, null if value
    boolean done;                                 // true when call is done

    protected Call(Writable param) {
      this.param = param;
      synchronized (Client.this) {
        this.id = counter++;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param value return value of the call.
     */
    public synchronized void setValue(Writable value) {
      this.value = value;
      callComplete();
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress server;             // server ip:port
    private String serverPrincipal;  // server's krb5 principal name
    private ConnectionHeader header;              // connection header
    private final ConnectionId remoteId;                // connection id
    private AuthMethod authMethod; // authentication method
    private boolean useSasl;
    private Token<? extends TokenIdentifier> token;
    private SaslRpcClient saslRpcClient;
    
    private Socket socket = null;                 // connected socket
    private DataInputStream in;
    private DataOutputStream out;
    private int rpcTimeout;
    private int maxIdleTime; //connections will be culled if it was idle for
         //maxIdleTime msecs
    private int maxRetries; //the max. no. of retries for socket connections
    private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private boolean doPing; //do we need to send ping message
    private int pingInterval; // how often sends ping to the server in msecs
    
    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason
    
    private final Object sendParamsLock = new Object();


    public Connection(ConnectionId remoteId) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw new UnknownHostException("unknown host: " + 
                                       remoteId.getAddress().getHostName());
      }
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.maxRetries = remoteId.getMaxRetries();
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.doPing = remoteId.getDoPing();
      this.pingInterval = remoteId.getPingInterval();
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is" + this.pingInterval + "ms.");
      }
      
      UserGroupInformation ticket = remoteId.getTicket();
      Class<?> protocol = remoteId.getProtocol();
      this.useSasl = UserGroupInformation.isSecurityEnabled();
      if (useSasl && protocol != null) {
        TokenInfo tokenInfo = protocol.getAnnotation(TokenInfo.class);
        if (tokenInfo != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector = null;
          try {
            tokenSelector = tokenInfo.value().newInstance();
          } catch (InstantiationException e) {
            throw new IOException(e.toString());
          } catch (IllegalAccessException e) {
            throw new IOException(e.toString());
          }
          InetSocketAddress addr = remoteId.getAddress();
          token = tokenSelector.selectToken(new Text(addr.getAddress()
              .getHostAddress() + ":" + addr.getPort()), 
              ticket.getTokens());
        }
        KerberosInfo krbInfo = protocol.getAnnotation(KerberosInfo.class);
        if (krbInfo != null) {
          serverPrincipal = remoteId.getServerPrincipal();
          if (LOG.isDebugEnabled()) {
            LOG.debug("RPC Server's Kerberos principal name for protocol="
                + protocol.getCanonicalName() + " is " + serverPrincipal);
          }
        }
      }
      
      if (!useSasl) {
        authMethod = AuthMethod.SIMPLE;
      } else if (token != null) {
        authMethod = AuthMethod.DIGEST;
      } else {
        authMethod = AuthMethod.KERBEROS;
      }
      
      header = new ConnectionHeader(protocol == null ? null : protocol
          .getName(), ticket, authMethod);
      
      if (LOG.isDebugEnabled())
        LOG.debug("Use " + authMethod + " authentication for protocol "
            + protocol.getSimpleName());
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          remoteId.getAddress().toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(System.currentTimeMillis());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or
       * is not configured to have a RPC timeout, send a ping.
       * (if rpcTimeout is not set to be 0, then RPC should timeout)
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
        } catch (IOException ignored) {
        }
      }
    }
    
    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser = 
        UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && 
          loginUser != null &&
          //Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() && 
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie). 
          (loginUser.equals(currentUser) || loginUser.equals(realUser))
          ) {
          return true;
      }
      return false;
    }
    
    private synchronized boolean setupSaslConnection(final InputStream in2, 
        final OutputStream out2) 
        throws IOException {
      saslRpcClient = new SaslRpcClient(authMethod, token, serverPrincipal);
      return saslRpcClient.saslConnect(in2, out2);
    }
    
    private synchronized void setupConnection() throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          // connection time out is 20s
          NetUtils.connect(this.socket, remoteId.getAddress(), 20000);
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout; // rpcTimeout overwrites pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (SocketTimeoutException toe) {
          /* The max number of retries is 45,
           * which amounts to 20s*45 = 15 minutes retries.
           */
          handleConnectionFailure(timeoutFailures++, 45, toe);
        } catch (IOException ie) {
          handleConnectionFailure(ioFailures++, maxRetries, ie);
        }
      }
    }
    /**
     * If multiple clients with the same principal try to connect 
     * to the same server at the same time, the server assumes a 
     * replay attack is in progress. This is a feature of kerberos.
     * In order to work around this, what is done is that the client
     * backs off randomly and tries to initiate the connection
     * again.
     * The other problem is to do with ticket expiry. To handle that,
     * a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries,
        final int maxRetries, final Exception ex, final Random rand, 
        final UserGroupInformation ugi) 
    throws IOException, InterruptedException{
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        public Object run() throws IOException, InterruptedException {
          final short maxBackoff = 5000;
          closeConnection();
          disposeSasl();
          //TODO: currRetries is overloaded here.For both kerberos and timeout 
          //failures, currRetries is used to maintain the number of failures 
          //seen so far. Should fix this to distinguish between the two.
          //TODO: Refactor the method to remove duplicated code (e.g.,the logic
          //inside "currRetries < maxRetries" could be factored out to apply
          //to both SocketTimeoutException and Kerberos exception...
          if (ex instanceof SocketTimeoutException) {
            if (currRetries < maxRetries) {
              LOG.warn("Encountered " + ex + " while trying to establish" +
              		" SASL connection to the server. Will retry SASL connection"+
              		" to server with principal " +
                  serverPrincipal);
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(maxBackoff) + 1));
              return null;
            } else {
              throw new IOException(ex);
            }
          }
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              LOG.debug("Exception encountered while connecting to " +
                  "the server : " + ex);
              //try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              //have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(maxBackoff) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for " + 
              UserGroupInformation.getLoginUser().getUserName() +
              " to " + serverPrincipal;
              LOG.warn(msg);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to " +
                "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException)ex;
          throw new IOException(ex);
        }
      });
    }
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    private synchronized void setupIOstreams() throws InterruptedException {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }
     
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        short numRetries = 0;
        final short maxRetries = 15;
        Random rand = null;
        while (true) {
          setupConnection();
          InputStream inStream = NetUtils.getInputStream(socket);
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeRpcHeader(outStream);
          if (useSasl) {
            final InputStream in2 = inStream;
            final OutputStream out2 = outStream;
            UserGroupInformation ticket = remoteId.getTicket();
            if (authMethod == AuthMethod.KERBEROS) {
              if (ticket.getRealUser() != null) {
                ticket = ticket.getRealUser();
              }
            }
            boolean continueSasl = false;
            try { 
              continueSasl = 
                ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
                  @Override
                  public Boolean run() throws IOException {
                    return setupSaslConnection(in2, out2);
                  }
                }); 
            } catch (Exception ex) {
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, maxRetries, ex, rand,
                   ticket);
              continue;
            }
            if (continueSasl) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
            } else {
              // fall back to simple auth because server told us so.
              authMethod = AuthMethod.SIMPLE;
              header = new ConnectionHeader(header.getProtocol(),
                  header.getUgi(), authMethod);
              useSasl = false;
            }
          }
          this.in = new DataInputStream(new BufferedInputStream
              (new PingInputStream(inStream)));
          this.out = new DataOutputStream
          (new BufferedOutputStream(outStream));
          writeHeader();

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (Throwable t) {
        if (t instanceof IOException) {
          markClosed((IOException)t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams", t));
        }
        close();
      }
    }
    
    private void closeConnection() {
      // close the current connection
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Not able to close a socket", e);
        }
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionFailure(
        int curRetries, int maxRetries, IOException ioe) throws IOException {
      
      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {}
      
      LOG.info("Retrying connect to server: " + server + 
          ". Already tried " + curRetries + " time(s).");
    }

    /* Write the RPC header */
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(Server.HEADER.array());
      out.write(Server.CURRENT_VERSION);
      authMethod.write(out);
      out.flush();
    }
    
    /* Write the protocol header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader() throws IOException {
      // Write out the ConnectionHeader
      DataOutputBuffer buf = new DataOutputBuffer();
      header.write(buf);
      
      // Write out the payload length
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
    }
    
    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing() throws IOException {
      long curTime = System.currentTimeMillis();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (out) {
          out.writeInt(PING_CALL_ID);
          out.flush();
        }
      }
    }

    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveResponse();
        }
      } catch (Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    public void sendParam(final Call call) throws InterruptedException {
      if (shouldCloseConnection.get()) {
        return;
      }
      
      // lock the connection for the period of submission and waiting
      // in order to bound the # of threads in the executor by the number
      // of connections
      synchronized (sendParamsLock) {
        Future senderFuture = SEND_PARAMS_EXECUTOR.submit(new Runnable() {
          @Override
          public void run() {
            DataOutputBuffer d = null;

            try {
              synchronized (Connection.this.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug(getName() + " sending #" + call.id);
                }

                //for serializing the
                //data to be written
                d = new DataOutputBuffer();
                d.writeInt(call.id);
                call.param.write(d);
                byte[] data = d.getData();
                int dataLength = d.getLength();
                out.writeInt(dataLength);      //first put the data length
                out.write(data, 0, dataLength);//write the data
                out.flush();
              }
            } catch (IOException e) {
              markClosed(e);
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              IOUtils.closeStream(d);
            }
          }
        });

        try {
          senderFuture.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();

          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else {
            throw new RuntimeException("checked exception made it here", cause);
          }
        }
      }
    }  

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        int id = in.readInt();                    // try to read an id

        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + id);

        Call call = calls.get(id);

        int state = in.readInt();     // read call status
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          call.setValue(value);
          calls.remove(id);
        } else if (state == Status.ERROR.state) {
          call.setException(new RemoteException(WritableUtils.readString(in),
                                                WritableUtils.readString(in)));
          calls.remove(id);
        } else if (state == Status.FATAL.state) {
          // Close the connection
          markClosed(new RemoteException(WritableUtils.readString(in), 
                                         WritableUtils.readString(in)));
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        c.setException(closeException); // local exception
        itor.remove();         
      }
    }
  }

  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private ParallelResults results;
    private int index;
    
    public ParallelCall(Writable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    protected void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    private Writable[] values;
    private int size;
    private int count;

    public ParallelResults(int size) {
      this.values = new Writable[size];
      this.size = size;
    }

    /** Collect a result. */
    public synchronized void callComplete(ParallelCall call) {
      values[call.index] = call.value;            // store the value
      count++;                                    // count it
      if (count == size)                          // if all values are in
        notify();                                 // then notify waiting caller
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @deprecated Use {@link #call(Writable, ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress address)
  throws InterruptedException, IOException {
      return call(param, address, null);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> with the <code>ticket</code> credentials, returning 
   * the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   * @deprecated Use {@link #call(Writable, ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr, 
      UserGroupInformation ticket)  
      throws InterruptedException, IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, null, ticket, 0,
        conf);
    return call(param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. 
   * @deprecated Use {@link #call(Writable, ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket)  
                       throws InterruptedException, IOException {
    return call(param, addr, protocol, ticket, 0);
  }
  
  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr,
                       Class<?> protocol, UserGroupInformation ticket,
                       int rpcTimeout)
                       throws InterruptedException, IOException {  
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials and <code>conf</code> as 
   * configuration for this connection, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. 
   * @throws IOException 
   * @throws InterruptedException */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket,
                       Configuration conf)
                       throws InterruptedException, IOException {  
    return call(param, addr, protocol, ticket, 0, conf);
  }

  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket,
                       int rpcTimeout, Configuration conf)  
                       throws InterruptedException, IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. */
  public Writable call(Writable param, ConnectionId remoteId)  
                       throws InterruptedException, IOException {
    Call call = new Call(param);
    Connection connection = getConnection(remoteId, call);
    try {
      connection.sendParam(call);                 // send the parameter
    } catch (RejectedExecutionException e) {
      throw new IOException("connection has been closed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("interrupted waiting to send params to server", e);
      throw new IOException(e);
    }

    boolean interrupted = false;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ie) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          throw wrapException(remoteId.getAddress(), call.error);
        }
      } else {
        return call.value;
      }
    }
  }

  /**
   * Take an IOException and the address we were trying to connect to
   * and return an IOException with the input exception as the cause.
   * The new exception provides the stack trace of the place where 
   * the exception is thrown and some extra diagnostics information.
   * If the exception is ConnectException or SocketTimeoutException, 
   * return a new one of the same type; Otherwise return an IOException.
   * 
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  private IOException wrapException(InetSocketAddress addr,
                                         IOException exception) {
    if (exception instanceof ConnectException) {
      //connection refused; include the host:port in the error
      return (ConnectException)new ConnectException(
           "Call to " + addr + " failed on connection exception: " + exception)
                    .initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException)new SocketTimeoutException(
           "Call to " + addr + " failed on socket timeout exception: "
                      + exception).initCause(exception);
    } else {
      return (IOException)new IOException(
           "Call to " + addr + " failed on local exception: " + exception)
                                 .initCause(exception);

    }
  }

  /** 
   * @deprecated Use {@link #call(Writable[], InetSocketAddress[], 
   * Class, UserGroupInformation, Configuration)} instead 
   */
  @Deprecated
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
    throws IOException, InterruptedException {
    return call(params, addresses, null, null, conf);
  }
  
  /**  
   * @deprecated Use {@link #call(Writable[], InetSocketAddress[], 
   * Class, UserGroupInformation, Configuration)} instead 
   */
  @Deprecated
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses, 
                         Class<?> protocol, UserGroupInformation ticket)
    throws IOException, InterruptedException {
    return call(params, addresses, protocol, ticket, conf);
  }
  

  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.  */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses,
      Class<?> protocol, UserGroupInformation ticket, Configuration conf)
      throws IOException, InterruptedException {
    if (addresses.length == 0) return new Writable[0];

    ParallelResults results = new ParallelResults(params.length);
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        try {
          ConnectionId remoteId = ConnectionId.getConnectionId(addresses[i],
              protocol, ticket, 0, conf);
          Connection connection = getConnection(remoteId, call);
          connection.sendParam(call);             // send each parameter
        } catch (RejectedExecutionException e) {
          throw new IOException("connection has been closed", e);
        } catch (IOException e) {
          // log errors
          LOG.info("Calling "+addresses[i]+" caught: " + 
                   e.getMessage(),e);
          results.size--;                         //  wait for one fewer result
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("interrupted waiting to send params to server", e);
          throw new IOException(e);
        }
      }
      while (results.count != results.size) {
        try {
          results.wait();                    // wait for all results
        } catch (InterruptedException e) {}
      }

      return results.values;
    }
  }

  //for unit testing only
  Set<ConnectionId> getConnectionIds() {
    synchronized (connections) {
      return connections.keySet();
    }
  }

  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
   private Connection getConnection(ConnectionId remoteId,
                                   Call call)
                                   throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));
    
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }

   /**
    * This class holds the address and the user ticket. The client connections
    * to servers are uniquely identified by <remoteAddress, protocol, ticket>
    */
   static class ConnectionId {
     InetSocketAddress address;
     UserGroupInformation ticket;
     Class<?> protocol;
     private static final int PRIME = 16777619;
     private int rpcTimeout;
     private String serverPrincipal;
     private int maxIdleTime; //connections will be culled if it was idle for 
     //maxIdleTime msecs
     private int maxRetries; //the max. no. of retries for socket connections
     private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
     private boolean doPing; //do we need to send ping message
     private int pingInterval; // how often sends ping to the server in msecs
     
     
     ConnectionId(InetSocketAddress address, Class<?> protocol, 
                  UserGroupInformation ticket, int rpcTimeout,
                  String serverPrincipal, int maxIdleTime, 
                  int maxRetries, boolean tcpNoDelay,
                  boolean doPing, int pingInterval) {
       this.protocol = protocol;
       this.address = address;
       this.ticket = ticket;
       this.rpcTimeout = rpcTimeout;
       this.serverPrincipal = serverPrincipal;
       this.maxIdleTime = maxIdleTime;
       this.maxRetries = maxRetries;
       this.tcpNoDelay = tcpNoDelay;
       this.doPing = doPing;
       this.pingInterval = pingInterval;
     }
     
     InetSocketAddress getAddress() {
       return address;
     }
     
     Class<?> getProtocol() {
       return protocol;
     }
     
     UserGroupInformation getTicket() {
       return ticket;
     }
     
     private int getRpcTimeout() {
       return rpcTimeout;
     }
     
     String getServerPrincipal() {
       return serverPrincipal;
     }
     
     int getMaxIdleTime() {
       return maxIdleTime;
     }
     
     int getMaxRetries() {
       return maxRetries;
     }
     
     boolean getTcpNoDelay() {
       return tcpNoDelay;
     }
     
     boolean getDoPing() {
       return doPing;
     }
     
     int getPingInterval() {
       return pingInterval;
     }
     
     static ConnectionId getConnectionId(InetSocketAddress addr,
         Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
         Configuration conf) throws IOException {
       String remotePrincipal = getRemotePrincipal(conf, addr, protocol);
       boolean doPing = conf.getBoolean("ipc.client.ping", true);
       return new ConnectionId(addr, protocol, ticket,
           rpcTimeout, remotePrincipal,
           conf.getInt("ipc.client.connection.maxidletime", 10000), // 10s
           conf.getInt("ipc.client.connect.max.retries", 10),
           conf.getBoolean("ipc.client.tcpnodelay", false),
           doPing,
           (doPing ? Client.getPingInterval(conf) : 0));
     }
     
     private static String getRemotePrincipal(Configuration conf,
         InetSocketAddress address, Class<?> protocol) throws IOException {
       if (!UserGroupInformation.isSecurityEnabled() || protocol == null) {
         return null;
       }
       KerberosInfo krbInfo = protocol.getAnnotation(KerberosInfo.class);
       if (krbInfo != null) {
         String serverKey = krbInfo.serverPrincipal();
         if (serverKey == null) {
           throw new IOException(
               "Can't obtain server Kerberos config key from protocol="
                   + protocol.getCanonicalName());
         }
         return SecurityUtil.getServerPrincipal(conf.get(serverKey), address
             .getAddress());
       }
       return null;
     }
     
     static boolean isEqual(Object a, Object b) {
       return a == null ? b == null : a.equals(b);
     }

     @Override
     public boolean equals(Object obj) {
       if (obj == this) {
         return true;
       }
       if (obj instanceof ConnectionId) {
         ConnectionId that = (ConnectionId) obj;
         return isEqual(this.address, that.address)
             && this.doPing == that.doPing
             && this.maxIdleTime == that.maxIdleTime
             && this.maxRetries == that.maxRetries
             && this.pingInterval == that.pingInterval
             && isEqual(this.protocol, that.protocol)
             && isEqual(this.serverPrincipal, that.serverPrincipal)
             && this.rpcTimeout == that.rpcTimeout
             && this.tcpNoDelay == that.tcpNoDelay
             && isEqual(this.ticket, that.ticket);
       }
       return false;
     }
     
     @Override
     public int hashCode() {
       int result = 1;
       result = PRIME * result + ((address == null) ? 0 : address.hashCode());
       result = PRIME * result + ((doPing ? 1231 : 1237));
       result = PRIME * result + maxIdleTime;
       result = PRIME * result + maxRetries;
       result = PRIME * result + pingInterval;
       result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
       result = PRIME * result + rpcTimeout;
       result = PRIME * result
           + ((serverPrincipal == null) ? 0 : serverPrincipal.hashCode());
       result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
       result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
       return result;
     }
   }  
}
