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
package org.apache.hadoop.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Random;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class TestServletFilter extends junit.framework.TestCase {
  static final Log LOG = LogFactory.getLog(HttpServer.class);
  static volatile String uri = null; 

  /** A very simple filter which record the uri filtered. */
  static public class SimpleFilter implements Filter {
    private FilterConfig filterConfig = null;

    public void init(FilterConfig filterConfig) {
      this.filterConfig = filterConfig;
    }

    public void destroy() {
      this.filterConfig = null;
    }

    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      if (filterConfig == null)
         return;

      uri = ((HttpServletRequest)request).getRequestURI();
      LOG.info("filtering " + uri);
      chain.doFilter(request, response);
    }

    /** Configuration for the filter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("simple", SimpleFilter.class.getName(), null);
      }
    }
  }
  
  
  /** access a url, ignoring some IOException such as the page does not exist */
  static void access(String urlstring) throws IOException {
    LOG.warn("access " + urlstring);
    URL url = new URL(urlstring);
    URLConnection connection = url.openConnection();
    connection.connect();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(
          connection.getInputStream()));
      try {
        for(; in.readLine() != null; );
      } finally {
        in.close();
      }
    } catch(IOException ioe) {
      LOG.warn("urlstring=" + urlstring, ioe);
    }
  }

  public void testServletFilter() throws Exception {
    Configuration conf = new Configuration();
    
    //start a http server with CountingFilter
    conf.set(HttpServer.FILTER_INITIALIZER_PROPERTY,
        SimpleFilter.Initializer.class.getName());
    HttpServer http = new HttpServer("datanode", "localhost", 0, true, conf);
    http.start();

    final String fsckURL = "/fsck";
    final String stacksURL = "/stacks";
    final String ajspURL = "/a.jsp";
    final String logURL = "/logs/a.log";
    final String hadooplogoURL = "/static/hadoop-logo.jpg";
    
    final String[] urls = {fsckURL, stacksURL, ajspURL, logURL, hadooplogoURL};
    final Random ran = new Random();
    final int[] sequence = new int[50];

    //generate a random sequence and update counts 
    for(int i = 0; i < sequence.length; i++) {
      sequence[i] = ran.nextInt(urls.length);
    }

    //access the urls as the sequence
    final String prefix = "http://localhost:" + http.getPort();
    try {
      for(int i = 0; i < sequence.length; i++) {
        access(prefix + urls[sequence[i]]);

        //make sure everything except fsck get filtered
        if (sequence[i] == 0) {
          assertEquals(null, uri);
        } else {
          assertEquals(urls[sequence[i]], uri);
          uri = null;
        }
      }
    } finally {
      http.stop();
    }
  }
}
