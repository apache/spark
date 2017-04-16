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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHttpServer {
  private HttpServer server;
  private URL baseUrl;
  
  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintWriter out = response.getWriter();
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet(params.keySet());
      for(String key: keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for(int i=1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }    
  }

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintWriter out = response.getWriter();
      SortedSet<String> sortedKeys = new TreeSet();
      Enumeration<String> keys = request.getParameterNames();
      while(keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for(String key: sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }    
  }

  @SuppressWarnings("serial")
  public static class HtmlContentServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      response.setContentType("text/html");
      PrintWriter out = response.getWriter();
      out.print("hello world");
      out.close();
    }
  }

  private String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }
  
  @Before public void setup() throws Exception {
    server = new TestWebappHttpServer("test", "0.0.0.0", 0, true);
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.addServlet("htmlcontent", "/htmlcontent", HtmlContentServlet.class);
    server.start();
    int port = server.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");
  }
  
  @After public void cleanup() throws Exception {
    server.stop();
  }

  @Test public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));    
  }
  
  /** Test the echo map servlet that uses getParameterMap. */
  @Test public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,&gt;\nc&lt;:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c<=d&a=>")));
  }

  @Test public void testContentTypes() throws Exception {
    // Static CSS files should have text/css
    URL cssUrl = new URL(baseUrl, "/static/test.css");
    HttpURLConnection conn = (HttpURLConnection)cssUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/css", conn.getContentType());

    // Servlets should have text/plain with proper encoding by default
    URL servletUrl = new URL(baseUrl, "/echo?a=b");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/plain; charset=utf-8", conn.getContentType());

    // We should ignore parameters for mime types - ie a parameter
    // ending in .css should not change mime type
    servletUrl = new URL(baseUrl, "/echo?a=b.css");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/plain; charset=utf-8", conn.getContentType());

    // Servlets that specify text/html should get that content type
    servletUrl = new URL(baseUrl, "/htmlcontent");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/html; charset=utf-8", conn.getContentType());

    // JSPs should default to text/html with utf8
    servletUrl = new URL(baseUrl, "/testjsp.jsp");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/html; charset=utf-8", conn.getContentType());
  }

  /**
   * Dummy filter that mimics as an authentication filter. Obtains user identity
   * from the request parameter user.name. Wraps around the request so that
   * request.getRemoteUser() returns the user identity.
   * 
   */
  public static class DummyServletFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(
        DummyServletFilter.class);
    @Override
    public void destroy() { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain filterChain) throws IOException, ServletException {
      final String userName = request.getParameter("user.name");
      ServletRequest requestModified =
        new HttpServletRequestWrapper((HttpServletRequest) request) {
        @Override
        public String getRemoteUser() {
          return userName;
        }
      };
      filterChain.doFilter(requestModified, response);
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException { }
  }

  /**
   * Subclass of HttpServer that loads the "test-webapps" directory
   * from the classpath instead of the "webapps" directory.
   * In trunk, HADOOP-6461 improves the HttpServer code to properly
   * find the right webapp dir, but that patch breaks downstream
   * implementors like HBase on 0.20.
   */
  static class TestWebappHttpServer extends HttpServer {
    public TestWebappHttpServer(String name, String bindAddress, int port,
                      boolean findPort, Configuration conf) throws IOException {
      super(name, bindAddress, port, findPort, conf);
    }
    public TestWebappHttpServer(String name, String bindAddress, int port,
                      boolean findPort) throws IOException {
      super(name, bindAddress, port, findPort);
    }
    public TestWebappHttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf, AccessControlList adminsAcl)
      throws IOException {
      super(name, bindAddress, port, findPort, conf, adminsAcl);
    }

    @Override
    protected String getWebAppsPath() throws IOException {
      URL url = getClass().getClassLoader().getResource("test-webapps");
      if (url == null) 
        throw new IOException("test-webapps not found in CLASSPATH"); 
      return url.toString();
    }
  }

  /**
   * FilterInitializer that initialized the DummyFilter.
   *
   */
  public static class DummyFilterInitializer extends FilterInitializer {
    public DummyFilterInitializer() {
    }

    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("DummyFilter", DummyServletFilter.class.getName(), null);
    }
  }

  /**
   * Access a URL and get the corresponding return Http status code. The URL
   * will be accessed as the passed user, by sending user.name request
   * parameter.
   * 
   * @param urlstring
   * @param userName
   * @return
   * @throws IOException
   */
  static int getHttpStatusCode(String urlstring, String userName)
      throws IOException {
    URL url = new URL(urlstring + "?user.name=" + userName);
    System.out.println("Accessing " + url + " as user " + userName);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.connect();
    return connection.getResponseCode();
  }

  /**
   * Custom user->group mapping service.
   */
  public static class MyGroupsProvider extends ShellBasedUnixGroupsMapping {
    static Map<String, List<String>> mapping = new HashMap<String, List<String>>();

    static void clearMapping() {
      mapping.clear();
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      return mapping.get(user);
    }
  }

  /**
   * Verify the access for /logs, /stacks, /conf, /logLevel and /metrics
   * servlets, when authentication filters are set, but authorization is not
   * enabled.
   * @throws Exception 
   */
  @Test
  public void testDisabledAuthorizationOfDefaultServlets() throws Exception {

    Configuration conf = new Configuration();

    // Authorization is disabled by default
    conf.set(HttpServer.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Arrays.asList("groupA"));
    MyGroupsProvider.mapping.put("userB", Arrays.asList("groupB"));

    HttpServer myServer = new TestWebappHttpServer("test", "0.0.0.0", 0, true, conf);
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    int port = myServer.getPort();
    String serverURL = "http://localhost:" + port + "/";
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      for (String user : new String[] { "userA", "userB" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
    }
    myServer.stop();
  }

  /**
   * Verify the administrator access for /logs, /stacks, /conf, /logLevel and
   * /metrics servlets.
   * 
   * @throws Exception
   */
  @Test
  public void testAuthorizationOfDefaultServlets() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
        true);
    conf.set(HttpServer.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Arrays.asList("groupA"));
    MyGroupsProvider.mapping.put("userB", Arrays.asList("groupB"));
    MyGroupsProvider.mapping.put("userC", Arrays.asList("groupC"));
    MyGroupsProvider.mapping.put("userD", Arrays.asList("groupD"));
    MyGroupsProvider.mapping.put("userE", Arrays.asList("groupE"));

    HttpServer myServer = new TestWebappHttpServer("test", "0.0.0.0", 0, true, conf,
        new AccessControlList("userA,userB groupC,groupD"));
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    int port = myServer.getPort();
    String serverURL = "http://localhost:" + port + "/";
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      for (String user : new String[] { "userA", "userB", "userC", "userD" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, getHttpStatusCode(
          serverURL + servlet, "userE"));
    }
    myServer.stop();
  }

  @Test
  public void testHasAdministratorAccess() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(null);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser()).thenReturn(null);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //authorization OFF
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

    //authorization ON & user NULL
    response = Mockito.mock(HttpServletResponse.class);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    Assert.assertFalse(HttpServer.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED), Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NULL
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getRemoteUser()).thenReturn("foo");
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

    //authorization ON & user NOT NULL & ACLs NOT NULL & user not in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED), Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NOT NULL & user in in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(true);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

  }

  @Test
  public void testRequiresAuthorizationAccess() throws Exception {
    Configuration conf = new Configuration();
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //requires admin access to instrumentation, FALSE by default
    Assert.assertTrue(HttpServer.isInstrumentationAccessAllowed(context, request, response));

    //requires admin access to instrumentation, TRUE
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer.isInstrumentationAccessAllowed(context, request, response));
  }

}
