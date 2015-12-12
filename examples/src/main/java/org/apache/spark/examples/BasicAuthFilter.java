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

package org.apache.spark.examples;

import com.sun.jersey.core.util.Base64;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.Filter;
import javax.servlet.FilterChain;

/**
 *  Servlet filter example for Spark web UI.
 *  BasicAuthFilter provides a functionality of basic authentication.
 *  The credentials of the filter are "spark-user:spark-password".
 *
 *  Usage:
 *
 *  $ $SPARK_HOME/bin/spark-submit \
 *      --jars examples/target/spark-examples_2.10-1.6.0-SNAPSHOT.jar \
 *      --master <Master URL of your Spark cluster> \
 *      --conf spark.ui.filters=org.apache.spark.examples.BasicAuthFilter
 *
 *  The web UI of your application will ask you username and password.
 *  Required credentials information is
 *    username: "spark-user"
 *    password: "spark-password"
 */
public class BasicAuthFilter implements Filter {
    String username = null;
    String password = null;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        this.username = "spark-user";
        this.password = "spark-password";
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest)servletRequest;
        HttpServletResponse response = (HttpServletResponse)servletResponse;

        String authHeader = request.getHeader("Authorization");
        if (authHeader != null) {
            StringTokenizer st = new StringTokenizer(authHeader);
            if (st.hasMoreTokens()) {
                String basic = st.nextToken();
                if (basic.equalsIgnoreCase("Basic")) {
                    try {
                        String credentials = new String(Base64.decode(st.nextToken()), "UTF-8");
                        int pos = credentials.indexOf(":");
                        if (pos != -1) {
                            String username = credentials.substring(0, pos).trim();
                            String password = credentials.substring(pos + 1).trim();

                            if (!username.equals(this.username) || !password.equals(this.password)) {
                                unauthorized(response, "Unauthorized:" +
                                        this.getClass().getCanonicalName());
                            }

                            filterChain.doFilter(servletRequest, servletResponse);
                        } else {
                            unauthorized(response, "Unauthorized:" +
                                    this.getClass().getCanonicalName());
                        }
                    } catch (UnsupportedEncodingException e) {
                        throw new Error("Counldn't retrieve authorization information", e);
                    }
                }
            }
        } else {
            unauthorized(response, "Unauthorized:" + this.getClass().getCanonicalName());
        }
    }

    @Override
    public void destroy() {}

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        response.setHeader("WWW-Authenticate", "Basic realm=\"Spark Realm\"");
        response.sendError(401, message);
    }

}
