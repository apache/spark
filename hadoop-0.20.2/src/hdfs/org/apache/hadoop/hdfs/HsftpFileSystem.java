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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.apache.hadoop.conf.Configuration;

/** An implementation of a protocol for accessing filesystems over HTTPS.
 * The following implementation provides a limited, read-only interface
 * to a filesystem over HTTPS.
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
public class HsftpFileSystem extends HftpFileSystem {

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setupSsl(conf);
  }

  /** Set up SSL resources */
  private static void setupSsl(Configuration conf) {
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get("dfs.https.client.keystore.resource",
        "ssl-client.xml"));
    System.setProperty("javax.net.ssl.trustStore", sslConf.get(
        "ssl.client.truststore.location", ""));
    System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
        "ssl.client.truststore.password", ""));
    System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
        "ssl.client.truststore.type", "jks"));
    System.setProperty("javax.net.ssl.keyStore", sslConf.get(
        "ssl.client.keystore.location", ""));
    System.setProperty("javax.net.ssl.keyStorePassword", sslConf.get(
        "ssl.client.keystore.password", ""));
    System.setProperty("javax.net.ssl.keyPassword", sslConf.get(
        "ssl.client.keystore.keypassword", ""));
    System.setProperty("javax.net.ssl.keyStoreType", sslConf.get(
        "ssl.client.keystore.type", "jks"));
  }

  @Override
  protected HttpURLConnection openConnection(String path, String query)
      throws IOException {
    query = addDelegationTokenParam(query);
    final URL url = new URL("https", nnAddr.getHostName(), 
        nnAddr.getPort(), path + '?' + query);
    HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
    // bypass hostname verification
    conn.setHostnameVerifier(new DummyHostnameVerifier());
    return (HttpURLConnection)conn;
  }

  @Override
  public URI getUri() {
    try {
      return new URI("hsftp", null, nnAddr.getHostName(), nnAddr.getPort(),
                     null, null, null);
    } catch (URISyntaxException e) {
      return null;
    } 
  }

  /**
   * Dummy hostname verifier that is used to bypass hostname checking
   */
  protected static class DummyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

}
