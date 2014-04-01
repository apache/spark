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
import java.math.BigInteger;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.net.InetSocketAddress;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

public class ProxyFilter implements Filter {
  public static final Log LOG = LogFactory.getLog(ProxyFilter.class);

  /** Pattern for triggering reload of user permissions */
  protected static final Pattern RELOAD_PATTERN = Pattern
      .compile("^(/reloadPermFiles)$");
  /** Pattern for a filter to find out if a request is HFTP/HSFTP request */
  protected static final Pattern HFTP_PATTERN = Pattern
      .compile("^(/listPaths|/data|/streamFile|/file)$");
  /**
   * Pattern for a filter to find out if an HFTP/HSFTP request stores its file
   * path in the extra path information associated with the URL; if not, the
   * file path is stored in request parameter "filename"
   */
  protected static final Pattern FILEPATH_PATTERN = Pattern
      .compile("^(/listPaths|/data|/file)$");

  private static volatile Map<String, Set<Path>> permsMap;
  private static volatile Map<String, Set<BigInteger>> certsMap;
  static {
    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    Map<String, Set<Path>> pMap = getPermMap(conf);
    permsMap = pMap != null ? pMap : new HashMap<String, Set<Path>>();
    Map<String, Set<BigInteger>> cMap = getCertsMap(conf);
    certsMap = cMap != null ? cMap : new HashMap<String, Set<BigInteger>>();
  }
  

  /** {@inheritDoc} */
  public void init(FilterConfig filterConfig) throws ServletException {
    ServletContext context = filterConfig.getServletContext();
    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    conf.addResource("ssl-server.xml");
    conf.addResource("hdfsproxy-site.xml");
    String nn = conf.get("hdfsproxy.dfs.namenode.address");
    if (nn == null) {
      throw new ServletException("Proxy source cluster name node address not speficied");
    }
    InetSocketAddress nAddr = NetUtils.createSocketAddr(nn);
    context.setAttribute("name.node.address", nAddr);
    context.setAttribute("name.conf", new Configuration());   
    
    context.setAttribute("org.apache.hadoop.hdfsproxy.conf", conf);
    LOG.info("proxyFilter initialization success: " + nn);
  }

  private static Map<String, Set<Path>> getPermMap(Configuration conf) {
    String permLoc = conf.get("hdfsproxy.user.permissions.file.location",
        "user-permissions.xml");
    if (conf.getResource(permLoc) == null) {
      LOG.warn("HdfsProxy user permissions file not found");
      return null;
    }
    Configuration permConf = new Configuration(false);
    permConf.addResource(permLoc);
    Map<String, Set<Path>> map = new HashMap<String, Set<Path>>();
    for (Map.Entry<String, String> e : permConf) {
      String k = e.getKey();
      String v = e.getValue();
      if (k != null && k.length() != 0 && v != null && v.length() != 0) {
        Set<Path> pathSet = new HashSet<Path>();
        String[] paths = v.split(",\\s*");
        for (String p : paths) {
          if (p.length() != 0) {
            pathSet.add(new Path(p));
          }
        }
        map.put(k, pathSet);
      }
    }
    return map;
  }

  private static Map<String, Set<BigInteger>> getCertsMap(Configuration conf) {
    String certsLoc = conf.get("hdfsproxy.user.certs.file.location",
        "user-certs.xml");
    if (conf.getResource(certsLoc) == null) {
      LOG.warn("HdfsProxy user certs file not found");
      return null;
    }
    Configuration certsConf = new Configuration(false);
    certsConf.addResource(certsLoc);
    Map<String, Set<BigInteger>> map = new HashMap<String, Set<BigInteger>>();
    for (Map.Entry<String, String> e : certsConf) {
      String k = e.getKey();
      String v = e.getValue().trim();
      if (k != null && k.length() != 0 && v != null && v.length() != 0) {
        Set<BigInteger> numSet = new HashSet<BigInteger>();
        String[] serialnumbers = v.split("\\s*,\\s*");
        for (String num : serialnumbers) {
          if (num.length() != 0) {
            numSet.add(new BigInteger(num, 16));
          }
        }
        map.put(k, numSet);
      }
    }
    return map;
  }

  /** {@inheritDoc} */
  public void destroy() {
  }
  
  

  /** {@inheritDoc} */
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    HttpServletRequest rqst = (HttpServletRequest) request;
    HttpServletResponse rsp = (HttpServletResponse) response;
    
    if (LOG.isDebugEnabled()) {
      StringBuilder b = new StringBuilder("Request from ").append(
          rqst.getRemoteHost()).append("/").append(rqst.getRemoteAddr())
          .append(":").append(rqst.getRemotePort());

      @SuppressWarnings("unchecked")
      Enumeration<String> e = rqst.getAttributeNames();
      for (; e.hasMoreElements();) {
        String attribute = e.nextElement();
        b.append("\n  " + attribute + " => " + rqst.getAttribute(attribute));
      }

      X509Certificate[] userCerts = (X509Certificate[]) rqst
          .getAttribute("javax.servlet.request.X509Certificate");
      if (userCerts != null)
        for (X509Certificate cert : userCerts)
          b.append("\n Client certificate Subject Name is "
              + cert.getSubjectX500Principal().getName());

      b.append("\n The Scheme is " + rqst.getScheme());
      b.append("\n The Auth Type is " + rqst.getAuthType());
      b.append("\n The Path Info is " + rqst.getPathInfo());
      b.append("\n The Translated Path Info is " + rqst.getPathTranslated());
      b.append("\n The Context Path is " + rqst.getContextPath());
      b.append("\n The Query String is " + rqst.getQueryString());
      b.append("\n The Remote User is " + rqst.getRemoteUser());
      b.append("\n The User Principal is " + rqst.getUserPrincipal());
      b.append("\n The Request URI is " + rqst.getRequestURI());
      b.append("\n The Request URL is " + rqst.getRequestURL());
      b.append("\n The Servlet Path is " + rqst.getServletPath());

      LOG.debug(b.toString());
    }
    
    boolean unitTest = false;
    if (rqst.getScheme().equalsIgnoreCase("http") && rqst.getParameter("UnitTest") != null) unitTest = true;
    
    if (rqst.getScheme().equalsIgnoreCase("https") || unitTest) {
      boolean isAuthorized = false;
      X509Certificate[] certs = (X509Certificate[]) rqst.getAttribute("javax.servlet.request.X509Certificate");
      
      if (unitTest) {
        try {
          LOG.debug("==> Entering https unit test");
          String SslPath = rqst.getParameter("SslPath");
          InputStream inStream = new FileInputStream(SslPath);
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          X509Certificate cert = (X509Certificate)cf.generateCertificate(inStream);
          inStream.close();          
          certs = new X509Certificate[] {cert};
        } catch (Exception e) {
          // do nothing here
        }
      } 
      
      if (certs == null || certs.length == 0) {
        rsp.sendError(HttpServletResponse.SC_BAD_REQUEST,
          "No client SSL certificate received");
        LOG.info("No Client SSL certificate received");
        return;       
      }
      for (X509Certificate cert : certs) {
        try {
          cert.checkValidity();
        } catch (CertificateExpiredException e) {
          LOG.info("Received cert for "
              + cert.getSubjectX500Principal().getName() + " expired");
          rsp
              .sendError(HttpServletResponse.SC_FORBIDDEN,
                  "Certificate expired");
          return;
        } catch (CertificateNotYetValidException e) {
          LOG.info("Received cert for "
              + cert.getSubjectX500Principal().getName() + " is not yet valid");
          rsp.sendError(HttpServletResponse.SC_FORBIDDEN,
              "Certificate is not yet valid");
          return;
        }
      }
      
      String[] tokens = certs[0].getSubjectX500Principal().getName().split(
          "\\s*,\\s*");
      String userID = null;
      for (String s : tokens) {
        if (s.startsWith("CN=")) {
          userID = s;
          break;
        }
      }
      if (userID == null || userID.length() < 4) {
        LOG.info("Can't retrieve user ID from SSL certificate");
        rsp.sendError(HttpServletResponse.SC_FORBIDDEN,
            "Can't retrieve user ID from SSL certificate");
        return;
      }
      userID = userID.substring(3);
      
      String servletPath = rqst.getServletPath();
      if (unitTest) { 
        servletPath = rqst.getParameter("TestSevletPathInfo");
        LOG.info("this is for unit test purpose only");
      }
      
      if (HFTP_PATTERN.matcher(servletPath).matches()) {
        // request is an HSFTP request
        if (FILEPATH_PATTERN.matcher(servletPath).matches()) {
          // file path as part of the URL
          isAuthorized = checkPath(userID, certs[0],
              rqst.getPathInfo() != null ? rqst.getPathInfo() : "/");
        } else {
          // file path is stored in "filename" parameter
          isAuthorized = checkPath(userID, certs[0], rqst
              .getParameter("filename"));
        }
      } else if (RELOAD_PATTERN.matcher(servletPath).matches()
          && checkUser("Admin", certs[0])) {
        Configuration conf = new Configuration(false);
        conf.addResource("hdfsproxy-default.xml");
        Map<String, Set<Path>> permsMap = getPermMap(conf);
        Map<String, Set<BigInteger>> certsMap = getCertsMap(conf);
        if (permsMap == null || certsMap == null) {
          LOG.warn("Permission files reloading failed");
          rsp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              "Permission files reloading failed");
          return;
        }
        ProxyFilter.permsMap = permsMap;
        ProxyFilter.certsMap = certsMap;
        LOG.info("User permissions and user certs files reloaded");
        rsp.setStatus(HttpServletResponse.SC_OK);
        return;
      }

      if (!isAuthorized) {
        rsp.sendError(HttpServletResponse.SC_FORBIDDEN, "Unauthorized access");
        return;
      }
      
      // request is authorized, set ugi for servlets
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userID);
      rqst.setAttribute("authorized.ugi", ugi);
      rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID", userID);
    } else if(rqst.getScheme().equalsIgnoreCase("http")) { // http request, set ugi for servlets, only for testing purposes
      String ugi = rqst.getParameter("ugi");
      if (ugi != null) {
        rqst.setAttribute("authorized.ugi", UserGroupInformation.createRemoteUser(ugi));
        rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID", ugi.split(",")[0]);
      }
    }
    chain.doFilter(request, response);
  }

  /** check that client's cert is listed in the user certs file */
  private boolean checkUser(String userID, X509Certificate cert) {
    Set<BigInteger> numSet = certsMap.get(userID);
    if (numSet == null) {
      LOG.info("User " + userID + " is not configured in the user certs file");
      return false;
    }
    if (!numSet.contains(cert.getSerialNumber())) {
      LOG.info("Cert with serial number " + cert.getSerialNumber()
          + " is not listed for user " + userID);
      return false;
    }
    return true;
  }

  /** check that the requested path is listed in the user permissions file */
  private boolean checkPath(String userID, X509Certificate cert, String pathInfo) {
    if (!checkUser(userID, cert)) {
      return false;
    }

    Set<Path> pathSet = permsMap.get(userID);
    if (pathSet == null) {
      LOG.info("User " + userID
              + " is not listed in the user permissions file");
      return false;
    }
    if (pathInfo == null || pathInfo.length() == 0) {
      LOG.info("Can't get file path from HTTPS request; user is " + userID);
      return false;
    }
    
    Path userPath = new Path(pathInfo);
    while (userPath != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("\n Checking file path " + userPath);
      }
      if (pathSet.contains(userPath))
        return true;
      userPath = userPath.getParent();
    }
    LOG.info("User " + userID + " is not authorized to access " + pathInfo);
    return false;
  }
}
