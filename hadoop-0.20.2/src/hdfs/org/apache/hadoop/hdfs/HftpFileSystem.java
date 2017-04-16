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

package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ServletUtil;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import org.apache.hadoop.hdfs.ByteRangeInputStream;

/** An implementation of a protocol for accessing filesystems over HTTP.
 * The following implementation provides a limited, read-only interface
 * to a filesystem over HTTP.
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
public class HftpFileSystem extends FileSystem {
  static {
    HttpURLConnection.setFollowRedirects(true);
  }

  private static final int DEFAULT_PORT = 50470;
  
  protected InetSocketAddress nnAddr;
  protected UserGroupInformation ugi; 
  private String nnHttpUrl;
  private URI hdfsURI;

  public static final String HFTP_TIMEZONE = "UTC";
  public static final String HFTP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  private Token<DelegationTokenIdentifier> delegationToken;
  public static final String HFTP_SERVICE_NAME_KEY = "hdfs.service.host_";

  public static final SimpleDateFormat getDateFormat() {
    final SimpleDateFormat df = new SimpleDateFormat(HFTP_DATE_FORMAT);
    df.setTimeZone(TimeZone.getTimeZone(HFTP_TIMEZONE));
    return df;
  }

  protected static final ThreadLocal<SimpleDateFormat> df =
    new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return getDateFormat();
      }
    };

  @Override
  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  @Override
  public String getCanonicalServiceName() {
    return SecurityUtil.buildDTServiceName(hdfsURI, getDefaultPort());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(final URI name, final Configuration conf) 
  throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    this.ugi = UserGroupInformation.getCurrentUser();

    nnAddr = NetUtils.createSocketAddr(name.toString());
    StringBuilder sb = new StringBuilder("https://");
    sb.append(NetUtils.normalizeHostName(name.getHost()));
    sb.append(":");
    sb.append(conf.getInt("dfs.https.port", DEFAULT_PORT));
    nnHttpUrl = sb.toString();
    
    String key = HftpFileSystem.HFTP_SERVICE_NAME_KEY+
      SecurityUtil.buildDTServiceName(name, DEFAULT_PORT);
    LOG.debug("Trying to find DT for " + name + " using key=" + key + 
        "; conf=" + conf.get(key, ""));
    String nnServiceName = conf.get(key);
    int nnPort = NameNode.DEFAULT_PORT;
    if (nnServiceName != null) {
      nnPort = NetUtils.createSocketAddr(nnServiceName, 
                                         NameNode.DEFAULT_PORT).getPort();
    }

    sb = new StringBuilder("hdfs://");
    sb.append(nnAddr.getHostName());
    sb.append(":");
    sb.append(nnPort);
    try {
      hdfsURI = new URI(sb.toString());
    } catch (URISyntaxException ue) {
      throw new IOException("bad uri for hdfs", ue);
    }
    
    if (UserGroupInformation.isSecurityEnabled()) {
      
      //try finding a token for this namenode (esp applicable for tasks
      //using hftp). If there exists one, just set the delegationField
      String canonicalName = getCanonicalServiceName();
      for (Token<? extends TokenIdentifier> t : ugi.getTokens()) {
        if (DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(t.getKind())&
            t.getService().toString().equals(canonicalName)) {
          LOG.debug("Found existing DT for " + name);
          delegationToken = (Token<DelegationTokenIdentifier>) t;
          break;
        }
      }
      //since we don't already have a token, go get one over https
      if (delegationToken == null) {
        delegationToken = 
          (Token<DelegationTokenIdentifier>) getDelegationToken(null);
        renewer.addTokenToRenew(this);
      }
    }
  }
  
  @Override
  public synchronized Token<?> getDelegationToken(final String renewer) throws IOException {
    try {
      //Renew TGT if needed
      ugi.checkTGTAndReloginFromKeytab();
      return ugi.doAs(new PrivilegedExceptionAction<Token<?>>() {
        public Token<?> run() throws IOException {
          Credentials c;
          try {
            c = DelegationTokenFetcher.getDTfromRemote(nnHttpUrl, renewer);
          } catch (Exception e) {
            LOG.info("Couldn't get a delegation token from " + nnHttpUrl + 
            " using https.");
            LOG.debug("error was ", e);
            //Maybe the server is in unsecure mode (that's bad but okay)
            return null;
          }
          for (Token<? extends TokenIdentifier> t : c.getAllTokens()) {
            LOG.debug("Got dt for " + getUri() + ";t.service="
                      +t.getService());
            t.setService(new Text(getCanonicalServiceName()));
            return t;
          }
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("hftp", null, nnAddr.getHostName(), nnAddr.getPort(),
                     null, null, null);
    } catch (URISyntaxException e) {
      return null;
    } 
  }
  
  /**
   * ugi parameter for http connection
   * 
   * @return user_shortname,group1,group2...
   */
  private String getEncodedUgiParameter() {
    StringBuilder ugiParamenter = new StringBuilder(
        ServletUtil.encodeQueryValue(ugi.getShortUserName()));
    for(String g: ugi.getGroupNames()) {
      ugiParamenter.append(",");
      ugiParamenter.append(ServletUtil.encodeQueryValue(g));
    }
    return ugiParamenter.toString();
  }

  /**
   * Return a URL pointing to given path on the namenode.
   *
   * @param path to obtain the URL for
   * @param query string to append to the path
   * @return namenode URL referring to the given path
   * @throws IOException on error constructing the URL
   */
  URL getNamenodeURL(String path, String query) throws IOException {
    final URL url = new URL("http", nnAddr.getHostName(),
        nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  /**
   * Open an HTTP connection to the namenode to read file data and metadata.
   * @param path The path component of the URL
   * @param query The query component of the URL
   */
  protected HttpURLConnection openConnection(String path, String query)
      throws IOException {
    query = addDelegationTokenParam(query);
    final URL url = getNamenodeURL(path, query);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    return connection;
  }

  protected String addDelegationTokenParam(String query) throws IOException {
    String tokenString = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      synchronized (this) {
        if (delegationToken != null) {
          tokenString = delegationToken.encodeToUrlString();
          return (query + JspHelper.getDelegationTokenUrlParam(tokenString));
        }
      }
    }
    return query;
  }

  @Override
  public FSDataInputStream open(Path f, int buffersize) throws IOException {
    String path = "/data" + ServletUtil.encodePath(f.toUri().getPath());
    String query = addDelegationTokenParam("ugi=" + getEncodedUgiParameter());
    URL u = getNamenodeURL(path, query);    
    return new FSDataInputStream(new ByteRangeInputStream(u));
  }

  /** Class to parse and store a listing reply from the server. */
  class LsParser extends DefaultHandler {

    ArrayList<FileStatus> fslist = new ArrayList<FileStatus>();

    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if ("listing".equals(qname)) return;
      if (!"file".equals(qname) && !"directory".equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }
      long modif;
      long atime = 0;
      try {
        final SimpleDateFormat ldf = df.get();
        modif = ldf.parse(attrs.getValue("modified")).getTime();
        String astr = attrs.getValue("accesstime");
        if (astr != null) {
          atime = ldf.parse(astr).getTime();
        }
      } catch (ParseException e) { throw new SAXException(e); }
      FileStatus fs = "file".equals(qname)
        ? new FileStatus(
              Long.valueOf(attrs.getValue("size")).longValue(), false,
              Short.valueOf(attrs.getValue("replication")).shortValue(),
              Long.valueOf(attrs.getValue("blocksize")).longValue(),
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HftpFileSystem.this))
        : new FileStatus(0L, true, 0, 0L,
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HftpFileSystem.this));
      fslist.add(fs);
    }
    
    private void fetchList(String path, boolean recur) throws IOException {
      try {
        XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        HttpURLConnection connection = openConnection(
            "/listPaths" + ServletUtil.encodePath(path),
            "ugi=" + getEncodedUgiParameter() + (recur ? "&recursive=yes" : ""));
        InputStream resp = connection.getInputStream();
        xr.parse(new InputSource(resp));
      } catch(SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("invalid xml directory content", e);
      }
    }

    public FileStatus getFileStatus(Path f) throws IOException {
      fetchList(f.toUri().getPath(), false);
      if (fslist.size() == 0) {
        throw new FileNotFoundException("File does not exist: " + f);
      }
      return fslist.get(0);
    }

    public FileStatus[] listStatus(Path f, boolean recur) throws IOException {
      fetchList(f.toUri().getPath(), recur);
      if (fslist.size() > 0 && (fslist.size() != 1 || fslist.get(0).isDir())) {
        fslist.remove(0);
      }
      return fslist.toArray(new FileStatus[0]);
    }

    public FileStatus[] listStatus(Path f) throws IOException {
      return listStatus(f, false);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.listStatus(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.getFileStatus(f);
  }

  private class ChecksumParser extends DefaultHandler {
    private FileChecksum filechecksum;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if (!MD5MD5CRC32FileChecksum.class.getName().equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }

      filechecksum = MD5MD5CRC32FileChecksum.valueOf(attrs);
    }

    private FileChecksum getFileChecksum(String f) throws IOException {
      final HttpURLConnection connection = openConnection(
          "/fileChecksum" + ServletUtil.encodePath(f), 
          "ugi=" + getEncodedUgiParameter());
      try {
        final XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        xr.parse(new InputSource(connection.getInputStream()));
      } catch(SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("invalid xml directory content", e);
      } finally {
        connection.disconnect();
      }
      return filechecksum;
    }
  }

  /** {@inheritDoc} */
  public FileChecksum getFileChecksum(Path f) throws IOException {
    final String s = makeQualified(f).toUri().getPath();
    return new ChecksumParser().getFileChecksum(s);
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/").makeQualified(this);
  }

  @Override
  public void setWorkingDirectory(Path f) { }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  /*
   * @deprecated Use delete(path, boolean)
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override 
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * A parser for parsing {@link ContentSummary} xml.
   */
  private class ContentSummaryParser extends DefaultHandler {
    private ContentSummary contentsummary;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if (!ContentSummary.class.getName().equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }

      contentsummary = toContentSummary(attrs);
    }

    /**
     * Connect to the name node and get content summary.  
     * @param path The path
     * @return The content summary for the path.
     * @throws IOException
     */
    private ContentSummary getContentSummary(String path) throws IOException {
      final HttpURLConnection connection = openConnection(
          "/contentSummary" + ServletUtil.encodePath(path), 
          "ugi=" + getEncodedUgiParameter());
      InputStream in = null;
      try {
        in = connection.getInputStream();        

        final XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        xr.parse(new InputSource(in));
      } catch(FileNotFoundException fnfe) {
        //the server may not support getContentSummary
        return null;
      } catch(SAXException saxe) {
        final Exception embedded = saxe.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("Invalid xml format", saxe);
      } finally {
        if (in != null) {
          in.close();
        }
        connection.disconnect();
      }
      return contentsummary;
    }
  }

  /** Return the object represented in the attributes. */
  private static ContentSummary toContentSummary(Attributes attrs
      ) throws SAXException {
    final String length = attrs.getValue("length");
    final String fileCount = attrs.getValue("fileCount");
    final String directoryCount = attrs.getValue("directoryCount");
    final String quota = attrs.getValue("quota");
    final String spaceConsumed = attrs.getValue("spaceConsumed");
    final String spaceQuota = attrs.getValue("spaceQuota");

    if (length == null
        || fileCount == null
        || directoryCount == null
        || quota == null
        || spaceConsumed == null
        || spaceQuota == null) {
      return null;
    }

    try {
      return new ContentSummary(
          Long.parseLong(length),
          Long.parseLong(fileCount),
          Long.parseLong(directoryCount),
          Long.parseLong(quota),
          Long.parseLong(spaceConsumed),
          Long.parseLong(spaceQuota));
    } catch(Exception e) {
      throw new SAXException("Invalid attributes: length=" + length
          + ", fileCount=" + fileCount
          + ", directoryCount=" + directoryCount
          + ", quota=" + quota
          + ", spaceConsumed=" + spaceConsumed
          + ", spaceQuota=" + spaceQuota, e);
    }
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    final String s = makeQualified(f).toUri().getPath();
    final ContentSummary cs = new ContentSummaryParser().getContentSummary(s);
    return cs != null? cs: super.getContentSummary(f);
  }
  
  /**
   * An action that will renew and replace the hftp file system's delegation 
   * tokens automatically.
   */
  private static class RenewAction implements Delayed {
    // when should the renew happen
    private long timestamp;
    // a weak reference to the file system so that it can be garbage collected
    private final WeakReference<HftpFileSystem> weakFs;

    RenewAction(long timestamp, HftpFileSystem fs) {
      this.timestamp = timestamp;
      this.weakFs = new WeakReference<HftpFileSystem>(fs);
    }
 
    /**
     * Get the delay until this event should happen.
     */
    @Override
    public long getDelay(TimeUnit unit) {
      long millisLeft = timestamp - System.currentTimeMillis();
      return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }

    /**
     * Compare two events in the same queue.
     */
    @Override
    public int compareTo(Delayed o) {
      if (o.getClass() != RenewAction.class) {
        throw new IllegalArgumentException("Illegal comparision to non-RenewAction");
      }
      RenewAction other = (RenewAction) o;
      return timestamp < other.timestamp ? -1 :
        (timestamp == other.timestamp ? 0 : 1);
    }
    
    /**
     * Set a new time for the renewal. Can only be called when the action
     * is not in the queue.
     * @param newTime the new time
     */
    public void setNewTime(long newTime) {
      timestamp = newTime;
    }

    /**
     * Renew or replace the delegation token for this file system.
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public boolean renew() throws IOException, InterruptedException {
      final HftpFileSystem fs = weakFs.get();
      if (fs != null) {
        synchronized (fs) {
          fs.ugi.checkTGTAndReloginFromKeytab();
          fs.ugi.doAs(new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
              try {
                DelegationTokenFetcher.renewDelegationToken(fs.nnHttpUrl, 
                                                            fs.delegationToken);
              } catch (IOException ie) {
                try {
                  fs.delegationToken = 
                    (Token<DelegationTokenIdentifier>) fs.getDelegationToken(null);
                } catch (IOException ie2) {
                  throw new IOException("Can't renew or get new delegation token ", 
                                        ie);
                }
              }
              return null;
            } 
          });
        }
      }
      return fs != null;
    }
    
    public String toString() {
      StringBuilder result = new StringBuilder();
      HftpFileSystem fs = weakFs.get();
      if (fs == null) {
        return "evaporated token renew";
      }
      synchronized (fs) {
        result.append(fs.delegationToken);
      }
      result.append(" renew in ");
      result.append(getDelay(TimeUnit.SECONDS));
      result.append(" secs");
      return result.toString();
    }
  }

  /**
   * A daemon thread that waits for the next file system to renew.
   */
  private static class RenewerThread extends Thread {
    private DelayQueue<RenewAction> queue = new DelayQueue<RenewAction>();
    // wait for 95% of a day between renewals
    private final int RENEW_CYCLE = (int) (0.95 * 24 * 60 * 60 * 1000);

    public RenewerThread() {
      super("HFTP Delegation Token Renewer");
      setDaemon(true);
    }

    public void addTokenToRenew(HftpFileSystem fs) {
      queue.add(new RenewAction(RENEW_CYCLE + System.currentTimeMillis(),fs));
    }

    public void run() {
      RenewAction action = null;
        while (true) {
          try {
            action = queue.take();
            if (action.renew()) {
              action.setNewTime(RENEW_CYCLE + System.currentTimeMillis());
              queue.add(action);
            }
            action = null;
          } catch (InterruptedException ie) {
            return;
          } catch (Exception ie) {
            if (action != null) {
              LOG.warn("Failure to renew token " + action, ie);
            } else {
              LOG.warn("Failure in renew queue", ie);
            }
          }
        }
    }
  }

  private static RenewerThread renewer = new RenewerThread();
  static {
    renewer.start();
  }
}
