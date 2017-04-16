/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.jmx;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpServer;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/*
 * This servlet is based off of the JMXProxyServlet from Tomcat 7.0.14. It has
 * been rewritten to be read only and to output in a JSON format so it is not
 * really that close to the original.
 */
/**
 * Provides Read only web access to JMX.
 * <p>
 * This servlet generally will be placed under the /jmx URL for each
 * HttpServer.  It provides read only
 * access to JMX metrics.  The optional <code>qry</code> parameter
 * may be used to query only a subset of the JMX Beans.  This query
 * functionality is provided through the
 * {@link MBeanServer#queryNames(ObjectName, javax.management.QueryExp)}
 * method.
 * <p>
 * For example <code>http://.../jmx?qry=Hadoop:*</code> will return
 * all hadoop metrics exposed through JMX.
 * <p>
 * The optional <code>get</code> parameter is used to query an specific 
 * attribute of a JMX bean.  The format of the URL is
 * <code>http://.../jmx?get=MXBeanName::AttributeName<code>
 * <p>
 * For example 
 * <code>
 * http://../jmx?get=Hadoop:service=NameNode,name=NameNodeInfo::ClusterId
 * </code> will return the cluster id of the namenode mxbean.
 * <p>
 * If the <code>qry</code> or the <code>get</code> parameter is not formatted 
 * correctly then a 400 BAD REQUEST http response code will be returned. 
 * <p>
 * If a resouce such as a mbean or attribute can not be found, 
 * a 404 SC_NOT_FOUND http response code will be returned. 
 * <p>
 * The return format is JSON and in the form
 * <p>
 *  <code><pre>
 *  {
 *    "beans" : [
 *      {
 *        "name":"bean-name"
 *        ...
 *      }
 *    ]
 *  }
 *  </pre></code>
 *  <p>
 *  The servlet attempts to convert the the JMXBeans into JSON. Each
 *  bean's attributes will be converted to a JSON object member.
 *  
 *  If the attribute is a boolean, a number, a string, or an array
 *  it will be converted to the JSON equivalent. 
 *  
 *  If the value is a {@link CompositeData} then it will be converted
 *  to a JSON object with the keys as the name of the JSON member and
 *  the value is converted following these same rules.
 *  
 *  If the value is a {@link TabularData} then it will be converted
 *  to an array of the {@link CompositeData} elements that it contains.
 *  
 *  All other objects will be converted to a string and output as such.
 *  
 *  The bean's name and modelerType will be returned for all beans.
 */
public class JMXJsonServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(JMXJsonServlet.class);

  private static final long serialVersionUID = 1L;

  // ----------------------------------------------------- Instance Variables
  /**
   * MBean server.
   */
  protected transient MBeanServer mBeanServer = null;

  // --------------------------------------------------------- Public Methods
  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
    // Retrieve the MBean server
    mBeanServer = ManagementFactory.getPlatformMBeanServer();
  }

  /**
   * Process a GET request for the specified resource.
   * 
   * @param request
   *          The servlet request we are processing
   * @param response
   *          The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                     request, response)) {
        return;
      }

      response.setContentType("application/json; charset=utf8");

      PrintWriter writer = response.getWriter();

      JsonFactory jsonFactory = new JsonFactory();
      JsonGenerator jg = jsonFactory.createJsonGenerator(writer);
      jg.useDefaultPrettyPrinter();
      jg.writeStartObject();
      if (mBeanServer == null) {
        jg.writeStringField("result", "ERROR");
        jg.writeStringField("message", "No MBeanServer could be found");
        jg.close();
        LOG.error("No MBeanServer could be found.");
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
      
      // query per mbean attribute
      String getmethod = request.getParameter("get");
      if (getmethod != null) {
        String[] splitStrings = getmethod.split("\\:\\:");
        if (splitStrings.length != 2) {
          jg.writeStringField("result", "ERROR");
          jg.writeStringField("message", "query format is not as expected.");
          jg.close();
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        listBeans(jg, new ObjectName(splitStrings[0]), splitStrings[1],
            response);
        jg.close();
        return;
      }

      // query per mbean
      String qry = request.getParameter("qry");
      if (qry == null) {
        qry = "*:*";
      }
      listBeans(jg, new ObjectName(qry), null, response);
      jg.close();

    } catch ( IOException e ) {
      LOG.error("Caught an exception while processing JMX request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch ( MalformedObjectNameException e ) {
      LOG.error("Caught an exception while processing JMX request", e);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  // --------------------------------------------------------- Private Methods
  private void listBeans(JsonGenerator jg, ObjectName qry, String attribute, 
      HttpServletResponse response) 
  throws IOException {
    LOG.debug("Listing beans for "+qry);
    Set<ObjectName> names = null;
    names = mBeanServer.queryNames(qry, null);

    jg.writeArrayFieldStart("beans");
    Iterator<ObjectName> it = names.iterator();
    while (it.hasNext()) {
      ObjectName oname = it.next();
      MBeanInfo minfo;
      String code = "";
      Object attributeinfo = null;
      try {
        minfo = mBeanServer.getMBeanInfo(oname);
        code = minfo.getClassName();
        String prs = "";
        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            prs = "modelerType";
            code = (String) mBeanServer.getAttribute(oname, prs);
          }
          if (attribute!=null) {
            prs = attribute;
            attributeinfo = mBeanServer.getAttribute(oname, prs);
          }
        } catch (AttributeNotFoundException e) {
          // If the modelerType attribute was not found, the class name is used
          // instead.
          LOG.error("getting attribute " + prs + " of " + oname
              + " threw an exception", e);
        } catch (MBeanException e) {
          // The code inside the attribute getter threw an exception so log it,
          // and fall back on the class name
          LOG.error("getting attribute " + prs + " of " + oname
              + " threw an exception", e);
        } catch (RuntimeException e) {
          // For some reason even with an MBeanException available to them
          // Runtime exceptionscan still find their way through, so treat them
          // the same as MBeanException
          LOG.error("getting attribute " + prs + " of " + oname
              + " threw an exception", e);
        } catch ( ReflectionException e ) {
          // This happens when the code inside the JMX bean (setter?? from the
          // java docs) threw an exception, so log it and fall back on the 
          // class name
          LOG.error("getting attribute " + prs + " of " + oname
              + " threw an exception", e);
        }
      } catch (InstanceNotFoundException e) {
        //Ignored for some reason the bean was not found so don't output it
        continue;
      } catch ( IntrospectionException e ) {
        // This is an internal error, something odd happened with reflection so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + qry
            + " with MBean " + oname, e);
        continue;
      } catch ( ReflectionException e ) {
        // This happens when the code inside the JMX bean threw an exception, so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + qry
            + " with MBean " + oname, e);
        continue;
      }

      jg.writeStartObject();
      jg.writeStringField("name", oname.toString());
      
      jg.writeStringField("modelerType", code);
      if ((attribute != null) && (attributeinfo == null)) {
        jg.writeStringField("result", "ERROR");
        jg.writeStringField("message", "No attribute with name " + attribute
            + " was found.");
        jg.writeEndObject();
        jg.writeEndArray();
        jg.close();
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
      
      if (attribute != null) {
        writeAttribute(jg, attribute, attributeinfo);
      } else {
        MBeanAttributeInfo attrs[] = minfo.getAttributes();
        for (int i = 0; i < attrs.length; i++) {
          writeAttribute(jg, oname, attrs[i]);
        }
      }
      jg.writeEndObject();
    }
    jg.writeEndArray();
  }

  private void writeAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo attr) throws IOException {
    if (!attr.isReadable()) {
      return;
    }
    String attName = attr.getName();
    if ("modelerType".equals(attName)) {
      return;
    }
    if (attName.indexOf("=") >= 0 || attName.indexOf(":") >= 0
        || attName.indexOf(" ") >= 0) {
      return;
    }
    Object value = null;
    try {
      value = mBeanServer.getAttribute(oname, attName);
    } catch (RuntimeMBeanException e) {
      // UnsupportedOperationExceptions happen in the normal course of business,
      // so no need to log them as errors all the time.
      if (e.getCause() instanceof UnsupportedOperationException) {
        LOG.debug("getting attribute "+attName+" of "+oname+" threw an exception", e);
      } else {
        LOG.error("getting attribute "+attName+" of "+oname+" threw an exception", e);
      }
      return;
    } catch (AttributeNotFoundException e) {
      //Ignored the attribute was not found, which should never happen because the bean
      //just told us that it has this attribute, but if this happens just don't output
      //the attribute.
      return;
    } catch (MBeanException e) {
      //The code inside the attribute getter threw an exception so log it, and
      // skip outputting the attribute
      LOG.error("getting attribute "+attName+" of "+oname+" threw an exception", e);
      return;
    } catch (RuntimeException e) {
      //For some reason even with an MBeanException available to them Runtime exceptions
      //can still find their way through, so treat them the same as MBeanException
      LOG.error("getting attribute "+attName+" of "+oname+" threw an exception", e);
      return;
    } catch (ReflectionException e) {
      //This happens when the code inside the JMX bean (setter?? from the java docs)
      //threw an exception, so log it and skip outputting the attribute
      LOG.error("getting attribute "+attName+" of "+oname+" threw an exception", e);
      return;
    } catch (InstanceNotFoundException e) {
      //Ignored the mbean itself was not found, which should never happen because we
      //just accessed it (perhaps something unregistered in-between) but if this
      //happens just don't output the attribute.
      return;
    }

    writeAttribute(jg, attName, value);
  }
  
  private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
    jg.writeFieldName(attName);
    writeObject(jg, value);
  }
  
  private void writeObject(JsonGenerator jg, Object value) throws IOException {
    if(value == null) {
      jg.writeNull();
    } else {
      Class<?> c = value.getClass();
      if (c.isArray()) {
        jg.writeStartArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(jg, item);
        }
        jg.writeEndArray();
      } else if(value instanceof Number) {
        Number n = (Number)value;
        jg.writeNumber(n.toString());
      } else if(value instanceof Boolean) {
        Boolean b = (Boolean)value;
        jg.writeBoolean(b);
      } else if(value instanceof CompositeData) {
        CompositeData cds = (CompositeData)value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        jg.writeStartObject();
        for(String key: keys) {
          writeAttribute(jg, key, cds.get(key));
        }
        jg.writeEndObject();
      } else if(value instanceof TabularData) {
        TabularData tds = (TabularData)value;
        jg.writeStartArray();
        for(Object entry : tds.values()) {
          writeObject(jg, entry);
        }
        jg.writeEndArray();
      } else {
        jg.writeString(value.toString());
      }
    }
  }
}
