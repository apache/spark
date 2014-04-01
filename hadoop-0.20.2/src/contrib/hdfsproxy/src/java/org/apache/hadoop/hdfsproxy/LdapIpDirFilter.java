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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.net.NetUtils;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.InitialLdapContext;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Hashtable;

public class LdapIpDirFilter implements Filter {
  public static final Log LOG = LogFactory.getLog(LdapIpDirFilter.class);

  private static String baseName;
  private static String hdfsIpSchemaStr;
  private static String hdfsIpSchemaStrPrefix;
  private static String hdfsUidSchemaStr;
  private static String hdfsPathSchemaStr;

  private InitialLdapContext lctx;

  private class LdapRoleEntry {
    String userId;
    ArrayList<Path> paths;

    void init(String userId, ArrayList<Path> paths) {
      this.userId = userId;
      this.paths = paths;
    }

    boolean contains(Path path) {
      return paths != null && paths.contains(path);
    }

    @Override
    public String toString() {
      return "LdapRoleEntry{" +
          ", userId='" + userId + '\'' +
          ", paths=" + paths +
          '}';
    }
  }

  protected String contextPath;

  public void initialize(String bName, InitialLdapContext ctx) {
    // hook to cooperate unit test
    baseName = bName;
    hdfsIpSchemaStr = "uniqueMember";
    hdfsIpSchemaStrPrefix = "cn=";
    hdfsUidSchemaStr = "uid";
    hdfsPathSchemaStr = "documentLocation";
    lctx = ctx;
  }

  /** {@inheritDoc} */
  public void init(FilterConfig filterConfig) throws ServletException {
    ServletContext context = filterConfig.getServletContext();

    contextPath = context.getContextPath();

    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    conf.addResource("hdfsproxy-site.xml");
    // extract namenode from source conf.
    String nn = getNamenode(conf);

    InetSocketAddress nAddr = NetUtils.createSocketAddr(nn);
    context.setAttribute("name.node.address", nAddr);
    context.setAttribute("name.conf", conf);
    context.setAttribute(JspHelper.CURRENT_CONF, conf);

    // for storing hostname <--> cluster mapping to decide which source cluster
    // to forward
    context.setAttribute("org.apache.hadoop.hdfsproxy.conf", conf);

    if (lctx == null) {
      Hashtable<String, String> env = new Hashtable<String, String>();
      env.put(InitialLdapContext.INITIAL_CONTEXT_FACTORY, conf.get(
          "hdfsproxy.ldap.initial.context.factory",
          "com.sun.jndi.ldap.LdapCtxFactory"));
      env.put(InitialLdapContext.PROVIDER_URL, conf
          .get("hdfsproxy.ldap.provider.url"));

      try {
        lctx = new InitialLdapContext(env, null);
      } catch (NamingException ne) {
        throw new ServletException("NamingException in initializing ldap"
            + ne.toString());
      }

      baseName = conf.get("hdfsproxy.ldap.role.base");
      hdfsIpSchemaStr = conf.get("hdfsproxy.ldap.ip.schema.string",
          "uniqueMember");
      hdfsIpSchemaStrPrefix = conf.get(
          "hdfsproxy.ldap.ip.schema.string.prefix", "cn=");
      hdfsUidSchemaStr = conf.get("hdfsproxy.ldap.uid.schema.string", "uid");
      hdfsPathSchemaStr = conf.get("hdfsproxy.ldap.hdfs.path.schema.string",
          "documentLocation");
    }
    LOG.info(contextPath + ":: LdapIpDirFilter initialization successful");
  }

  private String getNamenode(Configuration conf) throws ServletException {
    String nn = conf.get("fs.default.name");
     if (nn == null) {
       throw new ServletException(
           "Proxy source cluster name node address not specified");
     }
     return nn;
  }

  /** {@inheritDoc} */
  public void destroy() {
  }

  /** {@inheritDoc} */
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    String prevThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(contextPath);
      HttpServletRequest rqst = (HttpServletRequest) request;
      HttpServletResponse rsp = (HttpServletResponse) response;

      if (LOG.isDebugEnabled()) {
        StringBuilder b = new StringBuilder("Request from ").append(
            rqst.getRemoteHost()).append("/").append(rqst.getRemoteAddr())
            .append(":").append(rqst.getRemotePort());
        b.append("\n The Scheme is " + rqst.getScheme());
        b.append("\n The Path Info is " + rqst.getPathInfo());
        b.append("\n The Translated Path Info is " + rqst.getPathTranslated());
        b.append("\n The Context Path is " + rqst.getContextPath());
        b.append("\n The Query String is " + rqst.getQueryString());
        b.append("\n The Request URI is " + rqst.getRequestURI());
        b.append("\n The Request URL is " + rqst.getRequestURL());
        b.append("\n The Servlet Path is " + rqst.getServletPath());
        LOG.debug(b.toString());
      }
      LdapRoleEntry ldapent = new LdapRoleEntry();
      // check ip address
      String userIp = rqst.getRemoteAddr();
      try {
        boolean isAuthorized = getLdapRoleEntryFromUserIp(userIp, ldapent);
        if (!isAuthorized) {
          rsp.sendError(HttpServletResponse.SC_FORBIDDEN, "IP " + userIp
              + " is not authorized to access");
          return;
        }
      } catch (NamingException ne) {
        throw new IOException("NamingException while searching ldap"
            + ne.toString());
      }

      // since we cannot pass ugi object cross context as they are from
      // different classloaders in different war file, we have to use String attribute.
      rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID",
        ldapent.userId);
      rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.paths",
        ldapent.paths);

      LOG.info("User: " + ldapent.userId + " Request: " + rqst.getPathInfo() +
          " From: " + rqst.getRemoteAddr());

      chain.doFilter(request, response);
    } finally {
      Thread.currentThread().setName(prevThreadName);
    }
  }

  /**
   * check if client's ip is listed in the Ldap Roles if yes, return true and
   * update ldapent. if not, return false
   * */
  @SuppressWarnings("unchecked")
  private boolean getLdapRoleEntryFromUserIp(String userIp,
      LdapRoleEntry ldapent) throws NamingException {
    String ipMember = hdfsIpSchemaStrPrefix + userIp;
    Attributes matchAttrs = new BasicAttributes(true);
    matchAttrs.put(new BasicAttribute(hdfsIpSchemaStr, ipMember));
    matchAttrs.put(new BasicAttribute(hdfsUidSchemaStr));
    matchAttrs.put(new BasicAttribute(hdfsPathSchemaStr));

    String[] attrIDs = { hdfsUidSchemaStr, hdfsPathSchemaStr };

    NamingEnumeration<SearchResult> results = lctx.search(baseName, matchAttrs,
        attrIDs);
    if (results.hasMore()) {
      String userId = null;
      ArrayList<Path> paths = new ArrayList<Path>();
      SearchResult sr = results.next();
      Attributes attrs = sr.getAttributes();
      for (NamingEnumeration ne = attrs.getAll(); ne.hasMore();) {
        Attribute attr = (Attribute) ne.next();
        if (hdfsUidSchemaStr.equalsIgnoreCase(attr.getID())) {
          userId = (String) attr.get();
        } else if (hdfsPathSchemaStr.equalsIgnoreCase(attr.getID())) {
          for (NamingEnumeration e = attr.getAll(); e.hasMore();) {
            String pathStr = (String) e.next();
            paths.add(new Path(pathStr));
          }
        }
      }
      ldapent.init(userId, paths);
      if (LOG.isDebugEnabled()) LOG.debug(ldapent);
      return true;
    }
    LOG.info("Ip address " + userIp
        + " is not authorized to access the proxy server");
    return false;
  }
}
