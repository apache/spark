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

package org.apache.spark.jmx;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Avoid the effect of shade plugin, create a utils to build servlet
 */
public class JmxServletUtils {
  public static ServletContextHandler buildServlet(String contextPath, String pathSpec) {
    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SESSIONS);
    context.setContextPath(contextPath);
    context.addServlet(new ServletHolder(new JMXJsonServlet()), pathSpec);
    return context;
  }
}
