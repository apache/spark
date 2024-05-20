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

package org.apache.spark.deploy.yarn;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import jakarta.servlet.*;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

// This class is copied from Hadoop 3.4.0
// org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
//
// Modification:
// Migrate from javax.servlet to jakarta.servlet
// Copy constant string definitions to strip external dependency
//  - RM_HA_URLS
//  - PROXY_USER_COOKIE_NAME
@Public
public class AmIpFilter implements Filter {
  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(AmIpFilter.class);

  @Deprecated
  public static final String PROXY_HOST = "PROXY_HOST";
  @Deprecated
  public static final String PROXY_URI_BASE = "PROXY_URI_BASE";
  public static final String PROXY_HOSTS = "PROXY_HOSTS";
  public static final String PROXY_HOSTS_DELIMITER = ",";
  public static final String PROXY_URI_BASES = "PROXY_URI_BASES";
  public static final String PROXY_URI_BASES_DELIMITER = ",";
  private static final String PROXY_PATH = "/proxy";
  // RM_HA_URLS is defined in AmFilterInitializer in the original Hadoop code
  private static final String RM_HA_URLS = "RM_HA_URLS";
  // WebAppProxyServlet is defined in WebAppProxyServlet in the original Hadoop code
  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";
  // update the proxy IP list about every 5 min
  private static long updateInterval = TimeUnit.MINUTES.toMillis(5);

  private String[] proxyHosts;
  private Set<String> proxyAddresses = null;
  private long lastUpdate;
  @VisibleForTesting
  Map<String, String> proxyUriBases;
  String[] rmUrls = null;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    // Maintain for backwards compatibility
    if (conf.getInitParameter(PROXY_HOST) != null
        && conf.getInitParameter(PROXY_URI_BASE) != null) {
      proxyHosts = new String[]{conf.getInitParameter(PROXY_HOST)};
      proxyUriBases = new HashMap<>(1);
      proxyUriBases.put("dummy", conf.getInitParameter(PROXY_URI_BASE));
    } else {
      proxyHosts = conf.getInitParameter(PROXY_HOSTS)
        .split(PROXY_HOSTS_DELIMITER);

      String[] proxyUriBasesArr = conf.getInitParameter(PROXY_URI_BASES)
        .split(PROXY_URI_BASES_DELIMITER);
      proxyUriBases = new HashMap<>(proxyUriBasesArr.length);
      for (String proxyUriBase : proxyUriBasesArr) {
        try {
          URL url = new URL(proxyUriBase);
          proxyUriBases.put(url.getHost() + ":" + url.getPort(), proxyUriBase);
        } catch(MalformedURLException e) {
          LOG.warn(proxyUriBase + " does not appear to be a valid URL", e);
        }
      }
    }

    if (conf.getInitParameter(RM_HA_URLS) != null) {
      rmUrls = conf.getInitParameter(RM_HA_URLS).split(",");
    }
  }

  protected Set<String> getProxyAddresses() throws ServletException {
    long now = Time.monotonicNow();
    synchronized(this) {
      if (proxyAddresses == null || (lastUpdate + updateInterval) <= now) {
        proxyAddresses = new HashSet<>();
        for (String proxyHost : proxyHosts) {
          try {
            for (InetAddress add : InetAddress.getAllByName(proxyHost)) {
              LOG.debug("proxy address is: {}", add.getHostAddress());
              proxyAddresses.add(add.getHostAddress());
            }
            lastUpdate = now;
          } catch (UnknownHostException e) {
            LOG.warn("Could not locate " + proxyHost + " - skipping", e);
          }
        }
        if (proxyAddresses.isEmpty()) {
          throw new ServletException("Could not locate any of the proxy hosts");
        }
      }
      return proxyAddresses;
    }
  }

  @Override
  public void destroy() {
    // Empty
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp,
      FilterChain chain) throws IOException, ServletException {
    ProxyUtils.rejectNonHttpRequests(req);

    HttpServletRequest httpReq = (HttpServletRequest)req;
    HttpServletResponse httpResp = (HttpServletResponse)resp;

    LOG.debug("Remote address for request is: {}", httpReq.getRemoteAddr());

    if (!getProxyAddresses().contains(httpReq.getRemoteAddr())) {
      StringBuilder redirect = new StringBuilder(findRedirectUrl());

      redirect.append(httpReq.getRequestURI());

      int insertPoint = redirect.indexOf(PROXY_PATH);

      if (insertPoint >= 0) {
        // Add /redirect as the second component of the path so that the RM web
        // proxy knows that this request was a redirect.
        insertPoint += PROXY_PATH.length();
        redirect.insert(insertPoint, "/redirect");
      }
      // add the query parameters on the redirect if there were any
      String queryString = httpReq.getQueryString();
      if (queryString != null && !queryString.isEmpty()) {
        redirect.append("?");
        redirect.append(queryString);
      }

      ProxyUtils.sendRedirect(httpReq, httpResp, redirect.toString());
    } else {
      String user = null;

      if (httpReq.getCookies() != null) {
        for (Cookie c: httpReq.getCookies()) {
          if (PROXY_USER_COOKIE_NAME.equals(c.getName())){
            user = c.getValue();
            break;
          }
        }
      }
      if (user == null) {
        LOG.debug("Could not find {} cookie, so user will not be set",
            PROXY_USER_COOKIE_NAME);

        chain.doFilter(req, resp);
      } else {
        AmIpPrincipal principal = new AmIpPrincipal(user);
        ServletRequest requestWrapper = new AmIpServletRequestWrapper(httpReq,
            principal);

        chain.doFilter(requestWrapper, resp);
      }
    }
  }

  @VisibleForTesting
  public String findRedirectUrl() throws ServletException {
    String addr = null;
    if (proxyUriBases.size() == 1) {
      // external proxy or not RM HA
      addr = proxyUriBases.values().iterator().next();
    } else if (rmUrls != null) {
      for (String url : rmUrls) {
        String host = proxyUriBases.get(url);
        if (isValidUrl(host)) {
          addr = host;
          break;
        }
      }
    }

    if (addr == null) {
      throw new ServletException(
          "Could not determine the proxy server for redirection");
    }
    return addr;
  }

  @VisibleForTesting
  public boolean isValidUrl(String url) {
    boolean isValid = false;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      conn.connect();
      isValid = conn.getResponseCode() == HttpURLConnection.HTTP_OK;
      // If security is enabled, any valid RM which can give 401 Unauthorized is
      // good enough to access. Since AM doesn't have enough credential, auth
      // cannot be completed and hence 401 is fine in such case.
      if (!isValid && UserGroupInformation.isSecurityEnabled()) {
        isValid = (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED)
            || (conn.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN);
        return isValid;
      }
    } catch (Exception e) {
      LOG.warn("Failed to connect to " + url + ": " + e.toString());
    }
    return isValid;
  }

  @VisibleForTesting
  protected static void setUpdateInterval(long updateInterval) {
    AmIpFilter.updateInterval = updateInterval;
  }
}
