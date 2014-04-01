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

package org.apache.hadoop.hdfsproxy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;

/**
 * Proxy Utility .
 */
public class ProxyUtil {
  public static final Log LOG = LogFactory.getLog(ProxyUtil.class);
  private static final long MM_SECONDS_PER_DAY = 1000 * 60 * 60 * 24;
  private static final int CERT_EXPIRATION_WARNING_THRESHOLD = 30; // 30 days

  // warning

  private static enum UtilityOption {
    RELOAD("-reloadPermFiles"), GET("-get"), CHECKCERTS(
        "-checkcerts");

    private String name = null;

    private UtilityOption(String arg) {
      this.name = arg;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Dummy hostname verifier that is used to bypass hostname checking
   */
  private static class DummyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  /**
   * Dummy trustmanager that is used to bypass server certificate checking
   */
  private static class DummyTrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }

  private static HttpsURLConnection openConnection(String hostname, int port,
      String path) throws IOException {
    try {
      final URL url = new URI("https", null, hostname, port, path, null, null)
          .toURL();
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      // bypass hostname verification
      conn.setHostnameVerifier(new DummyHostnameVerifier());
      conn.setRequestMethod("GET");
      return conn;
    } catch (URISyntaxException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  private static void setupSslProps(Configuration conf) throws IOException {
    FileInputStream fis = null;
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      KeyManager[] kms = null;
      TrustManager[] tms = null;
      if (conf.get("ssl.client.keystore.location") != null) {
        // initialize default key manager with keystore file and pass
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        KeyStore ks = KeyStore.getInstance(conf.get("ssl.client.keystore.type",
            "JKS"));
        char[] ksPass = conf.get("ssl.client.keystore.password", "changeit")
            .toCharArray();
        fis = new FileInputStream(conf.get("ssl.client.keystore.location",
            "keystore.jks"));
        ks.load(fis, ksPass);
        kmf.init(ks, conf.get("ssl.client.keystore.keypassword", "changeit")
            .toCharArray());
        kms = kmf.getKeyManagers();
        fis.close();
        fis = null;
      }
      // initialize default trust manager with keystore file and pass
      if (conf.getBoolean("ssl.client.do.not.authenticate.server", false)) {
        // by pass trustmanager validation
        tms = new DummyTrustManager[] { new DummyTrustManager() };
      } else {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        KeyStore ts = KeyStore.getInstance(conf.get(
            "ssl.client.truststore.type", "JKS"));
        char[] tsPass = conf.get("ssl.client.truststore.password", "changeit")
            .toCharArray();
        fis = new FileInputStream(conf.get("ssl.client.truststore.location",
            "truststore.jks"));
        ts.load(fis, tsPass);
        tmf.init(ts);
        tms = tmf.getTrustManagers();
      }
      sc.init(kms, tms, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      throw new IOException("Could not initialize SSLContext", e);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
  }

  static InetSocketAddress getSslAddr(Configuration conf) throws IOException {
    String addr = conf.get("hdfsproxy.https.address");
    if (addr == null)
      throw new IOException("HdfsProxy address is not specified");
    return NetUtils.createSocketAddr(addr);
  }

  static boolean sendCommand(Configuration conf, String path)
      throws IOException {
    setupSslProps(conf);
    int sslPort = getSslAddr(conf).getPort();
    int err = 0;
    StringBuilder b = new StringBuilder();

    HostsFileReader hostsReader = new HostsFileReader(conf.get(
        "hdfsproxy.hosts", "hdfsproxy-hosts"), "");
    Set<String> hostsList = hostsReader.getHosts();
    for (String hostname : hostsList) {
      HttpsURLConnection connection = null;
      try {
        connection = openConnection(hostname, sslPort, path);
        connection.connect();
        if (LOG.isDebugEnabled()) {
          StringBuffer sb = new StringBuffer();
          X509Certificate[] clientCerts = (X509Certificate[]) connection
              .getLocalCertificates();
          if (clientCerts != null) {
            for (X509Certificate cert : clientCerts)
              sb.append("\n Client certificate Subject Name is "
                  + cert.getSubjectX500Principal().getName());
          } else {
            sb.append("\n No client certificates were found");
          }
          X509Certificate[] serverCerts = (X509Certificate[]) connection
              .getServerCertificates();
          if (serverCerts != null) {
            for (X509Certificate cert : serverCerts)
              sb.append("\n Server certificate Subject Name is "
                  + cert.getSubjectX500Principal().getName());
          } else {
            sb.append("\n No server certificates were found");
          }
          LOG.debug(sb.toString());
        }
        if (connection.getResponseCode() != HttpServletResponse.SC_OK) {
          b.append("\n\t" + hostname + ": " + connection.getResponseCode()
              + " " + connection.getResponseMessage());
          err++;
        }
      } catch (IOException e) {
        b.append("\n\t" + hostname + ": " + e.getLocalizedMessage());
        if (LOG.isDebugEnabled())
          LOG.debug("Exception happend for host " + hostname, e);
        err++;
      } finally {
        if (connection != null)
          connection.disconnect();
      }
    }
    if (err > 0) {
      System.err.print("Command failed on the following " + err + " host"
          + (err == 1 ? ":" : "s:") + b.toString() + "\n");
      return false;
    }
    return true;
  }

  static FSDataInputStream open(Configuration conf, String hostname, int port,
      String path) throws IOException {
    setupSslProps(conf);
    HttpURLConnection connection = null;
    connection = openConnection(hostname, port, path);
    connection.connect();
    final InputStream in = connection.getInputStream();
    return new FSDataInputStream(new FSInputStream() {
      public int read() throws IOException {
        return in.read();
      }

      public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
      }

      public void close() throws IOException {
        in.close();
      }

      public void seek(long pos) throws IOException {
        throw new IOException("Can't seek!");
      }

      public long getPos() throws IOException {
        throw new IOException("Position unknown!");
      }

      public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
      }
    });
  }

  static void checkServerCertsExpirationDays(Configuration conf,
      String hostname, int port) throws IOException {
    setupSslProps(conf);
    HttpsURLConnection connection = null;
    connection = openConnection(hostname, port, null);
    connection.connect();
    X509Certificate[] serverCerts = (X509Certificate[]) connection
        .getServerCertificates();
    Date curDate = new Date();
    long curTime = curDate.getTime();
    if (serverCerts != null) {
      for (X509Certificate cert : serverCerts) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n Server certificate Subject Name: "
            + cert.getSubjectX500Principal().getName());
        Date expDate = cert.getNotAfter();
        long expTime = expDate.getTime();
        int dayOffSet = (int) ((expTime - curTime) / MM_SECONDS_PER_DAY);
        sb.append(" have " + dayOffSet + " days to expire");
        if (dayOffSet < CERT_EXPIRATION_WARNING_THRESHOLD)
          LOG.warn(sb.toString());
        else
          LOG.info(sb.toString());
      }
    } else {
      LOG.info("\n No Server certs was found");
    }

    if (connection != null) {
      connection.disconnect();
    }
  }

  public static UserGroupInformation getProxyUGIFor(String userID) {
    LOG.debug("Proxying as " + userID);
    try {
      return UserGroupInformation.createProxyUser(userID, UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      throw new RuntimeException("Unable get current logged in user", e);
    }
  }

  public static String getNamenode(Configuration conf) throws ServletException {
    String nn = conf.get("fs.default.name");
     if (nn == null) {
       throw new ServletException(
           "Proxy source cluster name node address not specified");
     }
     return nn;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1
        || (!UtilityOption.RELOAD.getName().equalsIgnoreCase(args[0])
            && !UtilityOption.GET.getName().equalsIgnoreCase(args[0]) && !UtilityOption.CHECKCERTS
            .getName().equalsIgnoreCase(args[0]))
        || (UtilityOption.GET.getName().equalsIgnoreCase(args[0]) && args.length != 4)
        || (UtilityOption.CHECKCERTS.getName().equalsIgnoreCase(args[0]) && args.length != 3)) {
      System.err.println("Usage: ProxyUtil [" + UtilityOption.RELOAD.getName()
          + "] | ["
          + UtilityOption.GET.getName() + " <hostname> <#port> <path> ] | ["
          + UtilityOption.CHECKCERTS.getName() + " <hostname> <#port> ]");
      System.exit(0);
    }
    Configuration conf = new Configuration(false);
    conf.addResource("ssl-client.xml");
    conf.addResource("hdfsproxy-default.xml");

    if (UtilityOption.RELOAD.getName().equalsIgnoreCase(args[0])) {
      // reload user-certs.xml and user-permissions.xml files
      sendCommand(conf, "/reloadPermFiles");
    } else if (UtilityOption.CHECKCERTS.getName().equalsIgnoreCase(args[0])) {
      checkServerCertsExpirationDays(conf, args[1], Integer.parseInt(args[2]));
    } else {
      String hostname = args[1];
      int port = Integer.parseInt(args[2]);
      String path = args[3];
      InputStream in = open(conf, hostname, port, path);
      IOUtils.copyBytes(in, System.out, conf, false);
      in.close();
    }
  }

}
