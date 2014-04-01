package org.apache.hadoop.thriftfs;

import com.facebook.thrift.TException;
import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;
import com.facebook.thrift.transport.TTransportFactory;

// Include Generated code
import org.apache.hadoop.thriftfs.api.*;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

/**
 * ThriftHadoopFileSystem
 * A thrift wrapper around the Hadoop File System
 */
public class HadoopThriftServer extends ThriftHadoopFileSystem {

  static int serverPort = 0;                    // default port
  TServer    server = null;

  public static class HadoopThriftHandler implements ThriftHadoopFileSystem.Iface
  {

    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.thrift");

    // HDFS glue
    Configuration conf;
    FileSystem fs;
        
    // stucture that maps each Thrift object into an hadoop object
    private long nextId = new Random().nextLong();
    private HashMap<Long, Object> hadoopHash = new HashMap<Long, Object>();
    private Daemon inactivityThread = null;

    // Detect inactive session
    private static volatile long inactivityPeriod = 3600 * 1000; // 1 hr
    private static volatile long inactivityRecheckInterval = 60 * 1000;
    private static volatile boolean fsRunning = true;
    private static long now;

    // allow outsider to change the hadoopthrift path
    public void setOption(String key, String val) {
    }

    /**
     * Current system time.
     * @return current time in msec.
     */
    static long now() {
      return System.currentTimeMillis();
    }

    /**
    * getVersion
    *
    * @return current version of the interface.
    */
    public String getVersion() {
      return "0.1";
    }

    /**
     * shutdown
     *
     * cleanly closes everything and exit.
     */
    public void shutdown(int status) {
      LOG.info("HadoopThriftServer shutting down.");
      try {
        fs.close();
      } catch (IOException e) {
        LOG.warn("Unable to close file system");
      }
      Runtime.getRuntime().exit(status);
    }

    /**
     * Periodically checks to see if there is inactivity
     */
    class InactivityMonitor implements Runnable {
      public void run() {
        while (fsRunning) {
          try {
            if (now() > now + inactivityPeriod) {
              LOG.warn("HadoopThriftServer Inactivity period of " +
                       inactivityPeriod + " expired... Stopping Server.");
              shutdown(-1);
            }
          } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
          }
          try {
            Thread.sleep(inactivityRecheckInterval);
          } catch (InterruptedException ie) {
          }
        }
      }
    }

    /**
     * HadoopThriftServer
     *
     * Constructor for the HadoopThriftServer glue with Thrift Class.
     *
     * @param name - the name of this handler
     */
    public HadoopThriftHandler(String name) {
      conf = new Configuration();
      now = now();
      try {
        inactivityThread = new Daemon(new InactivityMonitor());
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        LOG.warn("Unable to open hadoop file system...");
        Runtime.getRuntime().exit(-1);
      }
    }

    /**
      * printStackTrace
      *
      * Helper function to print an exception stack trace to the log and not stderr
      *
      * @param e the exception
      *
      */
    static private void printStackTrace(Exception e) {
      for(StackTraceElement s: e.getStackTrace()) {
        LOG.error(s);
      }
    }

    /**
     * Lookup a thrift object into a hadoop object
     */
    private synchronized Object lookup(long id) {
      return hadoopHash.get(new Long(id));
    }

    /**
     * Insert a thrift object into a hadoop object. Return its id.
     */
    private synchronized long insert(Object o) {
      nextId++;
      hadoopHash.put(nextId, o);
      return nextId;
    }

    /**
     * Delete a thrift object from the hadoop store.
     */
    private synchronized Object remove(long id) {
      return hadoopHash.remove(new Long(id));
    }

    /**
      * Implement the API exported by this thrift server
      */

    /** Set inactivity timeout period. The period is specified in seconds.
      * if there are no RPC calls to the HadoopThrift server for this much
      * time, then the server kills itself.
      */
    public synchronized void setInactivityTimeoutPeriod(long periodInSeconds) {
      inactivityPeriod = periodInSeconds * 1000; // in milli seconds
      if (inactivityRecheckInterval > inactivityPeriod ) {
        inactivityRecheckInterval = inactivityPeriod;
      }
    }


    /**
      * Create a file and open it for writing
      */
    public ThriftHandle create(Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("create: " + path);
        FSDataOutputStream out = fs.create(new Path(path.pathname));
        long id = insert(out);
        ThriftHandle obj = new ThriftHandle(id);
        HadoopThriftHandler.LOG.debug("created: " + path + " id: " + id);
        return obj;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
      * Create a file and open it for writing, delete file if it exists
      */
    public ThriftHandle createFile(Pathname path, 
                                   short mode,
                                   boolean  overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("create: " + path +
                                     " permission: " + mode +
                                     " overwrite: " + overwrite +
                                     " bufferSize: " + bufferSize +
                                     " replication: " + replication +
                                     " blockSize: " + blockSize);
        FSDataOutputStream out = fs.create(new Path(path.pathname), 
                                           new FsPermission(mode),
                                           overwrite,
                                           bufferSize,
                                           replication,
                                           blockSize,
                                           null); // progress
        long id = insert(out);
        ThriftHandle obj = new ThriftHandle(id);
        HadoopThriftHandler.LOG.debug("created: " + path + " id: " + id);
        return obj;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Opens an existing file and returns a handle to read it
     */
    public ThriftHandle open(Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("open: " + path);
        FSDataInputStream out = fs.open(new Path(path.pathname));
        long id = insert(out);
        ThriftHandle obj = new ThriftHandle(id);
        HadoopThriftHandler.LOG.debug("opened: " + path + " id: " + id);
        return obj;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Opens an existing file to append to it.
     */
    public ThriftHandle append(Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("append: " + path);
        FSDataOutputStream out = fs.append(new Path(path.pathname));
        long id = insert(out);
        ThriftHandle obj = new ThriftHandle(id);
        HadoopThriftHandler.LOG.debug("appended: " + path + " id: " + id);
        return obj;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * write to a file
     */
    public boolean write(ThriftHandle tout, String data) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("write: " + tout.id);
        FSDataOutputStream out = (FSDataOutputStream)lookup(tout.id);
        byte[] tmp = data.getBytes("UTF-8");
        out.write(tmp, 0, tmp.length);
        HadoopThriftHandler.LOG.debug("wrote: " + tout.id);
        return true;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * read from a file
     */
    public String read(ThriftHandle tout, long offset,
                       int length) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("read: " + tout.id +
                                     " offset: " + offset +
                                     " length: " + length);
        FSDataInputStream in = (FSDataInputStream)lookup(tout.id);
        if (in.getPos() != offset) {
          in.seek(offset);
        }
        byte[] tmp = new byte[length];
        int numbytes = in.read(offset, tmp, 0, length);
        HadoopThriftHandler.LOG.debug("read done: " + tout.id);
        return new String(tmp, 0, numbytes, "UTF-8");
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Delete a file/directory
     */
    public boolean rm(Pathname path, boolean recursive) 
                          throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("rm: " + path +
                                     " recursive: " + recursive);
        boolean ret = fs.delete(new Path(path.pathname), recursive);
        HadoopThriftHandler.LOG.debug("rm: " + path);
        return ret;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Move a file/directory
     */
    public boolean rename(Pathname path, Pathname dest) 
                          throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("rename: " + path +
                                     " destination: " + dest);
        boolean ret = fs.rename(new Path(path.pathname), 
                                new Path(dest.pathname));
        HadoopThriftHandler.LOG.debug("rename: " + path);
        return ret;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     *  close file
     */
     public boolean close(ThriftHandle tout) throws ThriftIOException {
       try {
         now = now();
         HadoopThriftHandler.LOG.debug("close: " + tout.id);
         Object obj = remove(tout.id);
         if (obj instanceof FSDataOutputStream) {
           FSDataOutputStream out = (FSDataOutputStream)obj;
           out.close();
         } else if (obj instanceof FSDataInputStream) {
           FSDataInputStream in = (FSDataInputStream)obj;
           in.close();
         } else {
           throw new ThriftIOException("Unknown thrift handle.");
         }
         HadoopThriftHandler.LOG.debug("closed: " + tout.id);
         return true;
       } catch (IOException e) {
         throw new ThriftIOException(e.getMessage());
       }
     }

     /**
      * Create a directory
      */
    public boolean mkdirs(Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("mkdirs: " + path);
        boolean ret = fs.mkdirs(new Path(path.pathname));
        HadoopThriftHandler.LOG.debug("mkdirs: " + path);
        return ret;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Does this pathname exist?
     */
    public boolean exists(Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("exists: " + path);
        boolean ret = fs.exists(new Path(path.pathname));
        HadoopThriftHandler.LOG.debug("exists done: " + path);
        return ret;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Returns status about the specified pathname
     */
    public org.apache.hadoop.thriftfs.api.FileStatus stat(
                            Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("stat: " + path);
        org.apache.hadoop.fs.FileStatus stat = fs.getFileStatus(
                                           new Path(path.pathname));
        HadoopThriftHandler.LOG.debug("stat done: " + path);
        return new org.apache.hadoop.thriftfs.api.FileStatus(
          stat.getPath().toString(),
          stat.getLen(),
          stat.isDir(),
          stat.getReplication(),
          stat.getBlockSize(),
          stat.getModificationTime(),
          stat.getPermission().toString(),
          stat.getOwner(),
          stat.getGroup());
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * If the specified pathname is a directory, then return the
     * list of pathnames in this directory
     */
    public List<org.apache.hadoop.thriftfs.api.FileStatus> listStatus(
                            Pathname path) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("listStatus: " + path);

        org.apache.hadoop.fs.FileStatus[] stat = fs.listStatus(
                                           new Path(path.pathname));
        HadoopThriftHandler.LOG.debug("listStatus done: " + path);
        org.apache.hadoop.thriftfs.api.FileStatus tmp;
        List<org.apache.hadoop.thriftfs.api.FileStatus> value = 
          new LinkedList<org.apache.hadoop.thriftfs.api.FileStatus>();

        for (int i = 0; i < stat.length; i++) {
          tmp = new org.apache.hadoop.thriftfs.api.FileStatus(
                      stat[i].getPath().toString(),
                      stat[i].getLen(),
                      stat[i].isDir(),
                      stat[i].getReplication(),
                      stat[i].getBlockSize(),
                      stat[i].getModificationTime(),
                      stat[i].getPermission().toString(),
                      stat[i].getOwner(),
                      stat[i].getGroup());
          value.add(tmp);
        }
        return value;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Sets the permission of a pathname
     */
    public void chmod(Pathname path, short mode) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("chmod: " + path + 
                                     " mode " + mode);
        fs.setPermission(new Path(path.pathname), new FsPermission(mode));
        HadoopThriftHandler.LOG.debug("chmod done: " + path);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Sets the owner & group of a pathname
     */
    public void chown(Pathname path, String owner, String group) 
                                                       throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("chown: " + path +
                                     " owner: " + owner +
                                     " group: " + group);
        fs.setOwner(new Path(path.pathname), owner, group);
        HadoopThriftHandler.LOG.debug("chown done: " + path);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    /**
     * Sets the replication factor of a file
     */
    public void setReplication(Pathname path, short repl) throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("setrepl: " + path +
                                     " replication factor: " + repl);
        fs.setReplication(new Path(path.pathname), repl);
        HadoopThriftHandler.LOG.debug("setrepl done: " + path);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }

    }

    /**
     * Returns the block locations of this file
     */
    public List<org.apache.hadoop.thriftfs.api.BlockLocation> 
             getFileBlockLocations(Pathname path, long start, long length) 
                                         throws ThriftIOException {
      try {
        now = now();
        HadoopThriftHandler.LOG.debug("getFileBlockLocations: " + path);

        org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(
                                                 new Path(path.pathname));

        org.apache.hadoop.fs.BlockLocation[] stat = 
            fs.getFileBlockLocations(status, start, length);
        HadoopThriftHandler.LOG.debug("getFileBlockLocations done: " + path);

        org.apache.hadoop.thriftfs.api.BlockLocation tmp;
        List<org.apache.hadoop.thriftfs.api.BlockLocation> value = 
          new LinkedList<org.apache.hadoop.thriftfs.api.BlockLocation>();

        for (int i = 0; i < stat.length; i++) {

          // construct the list of hostnames from the array returned
          // by HDFS
          List<String> hosts = new LinkedList<String>();
          String[] hostsHdfs = stat[i].getHosts();
          for (int j = 0; j < hostsHdfs.length; j++) {
            hosts.add(hostsHdfs[j]);
          }

          // construct the list of host:port from the array returned
          // by HDFS
          List<String> names = new LinkedList<String>();
          String[] namesHdfs = stat[i].getNames();
          for (int j = 0; j < namesHdfs.length; j++) {
            names.add(namesHdfs[j]);
          }
          tmp = new org.apache.hadoop.thriftfs.api.BlockLocation(
                      hosts, names, stat[i].getOffset(), stat[i].getLength());
          value.add(tmp);
        }
        return value;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }
  }

  // Bind to port. If the specified port is 0, then bind to random port.
  private ServerSocket createServerSocket(int port) throws IOException {
    try {
      ServerSocket sock = new ServerSocket();
      // Prevent 2MSL delay problem on server restarts
      sock.setReuseAddress(true);
      // Bind to listening port
      if (port == 0) {
        sock.bind(null);
        serverPort = sock.getLocalPort();
      } else {
        sock.bind(new InetSocketAddress(port));
      }
      return sock;
    } catch (IOException ioe) {
      throw new IOException("Could not create ServerSocket on port " + port + "." +
                            ioe);
    }
  }

  /**
   * Constrcts a server object
   */
  public HadoopThriftServer(String [] args) {

    if (args.length > 0) {
      serverPort = new Integer(args[0]);
    }
    try {
      ServerSocket ssock = createServerSocket(serverPort);
      TServerTransport serverTransport = new TServerSocket(ssock);
      Iface handler = new HadoopThriftHandler("hdfs-thrift-dhruba");
      ThriftHadoopFileSystem.Processor processor = new ThriftHadoopFileSystem.Processor(handler);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = 10;
      server = new TThreadPoolServer(processor, serverTransport,
                                             new TTransportFactory(),
                                             new TTransportFactory(),
                                             new TBinaryProtocol.Factory(),
                                             new TBinaryProtocol.Factory(), 
                                             options);
      System.out.println("Starting the hadoop thrift server on port [" + serverPort + "]...");
      HadoopThriftHandler.LOG.info("Starting the hadoop thrift server on port [" +serverPort + "]...");
      System.out.flush();

    } catch (Exception x) {
      x.printStackTrace();
    }
  }

  public static void main(String [] args) {
    HadoopThriftServer me = new HadoopThriftServer(args);
    me.server.serve();
  }
};

