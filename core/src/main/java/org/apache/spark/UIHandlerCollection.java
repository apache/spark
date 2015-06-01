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
package org.apache.spark;

import org.eclipse.jetty.http.PathMap;
import org.eclipse.jetty.server.AsyncContinuation;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HandlerContainer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.LazyList;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UIHandlerCollection extends ContextHandlerCollection {
    private static final Logger LOG = Log.getLogger(ContextHandlerCollection.class);
    private volatile PathMap _contextMap;
    private int flag = 0;

    public UIHandlerCollection() {
        super();
    }

    public void mapContexts() {
        PathMap contextMap = new PathMap();
        Handler[] branches = this.getHandlers();

        for (int b = 0; branches != null && b < branches.length; ++b) {
            Handler[] handlers = null;
            if (branches[b] instanceof ContextHandler) {
                handlers = new Handler[]{branches[b]};
            } else {
                if (!(branches[b] instanceof HandlerContainer)) {
                    continue;
                }

                handlers = ((HandlerContainer) branches[b]).getChildHandlersByClass(ContextHandler.class);
            }

            for (int i = 0; i < handlers.length; ++i) {
                ContextHandler handler = (ContextHandler) handlers[i];
                String contextPath = handler.getContextPath();
                if (contextPath == null || contextPath.indexOf(44) >= 0 || contextPath.startsWith("*")) {
                    throw new IllegalArgumentException("Illegal context spec:" + contextPath);
                }

                if (!contextPath.startsWith("/")) {
                    contextPath = '/' + contextPath;
                }

                if (contextPath.length() > 1) {
                    if (contextPath.endsWith("/")) {
                        contextPath = contextPath + "*";
                    } else if (!contextPath.endsWith("/*")) {
                        contextPath = contextPath + "/*";
                    }
                }

                Object contexts = contextMap.get(contextPath);
                String[] vhosts = handler.getVirtualHosts();
                if (vhosts != null && vhosts.length > 0) {
                    Object var13;
                    if (contexts instanceof Map) {
                        var13 = (Map) contexts;
                    } else {
                        var13 = new HashMap();
                        ((Map) var13).put("*", contexts);
                        contextMap.put(contextPath, var13);
                    }

                    for (int j = 0; j < vhosts.length; ++j) {
                        String vhost = vhosts[j];
                        contexts = ((Map) var13).get(vhost);
                        contexts = LazyList.add(contexts, branches[b]);
                        ((Map) var13).put(vhost, contexts);
                    }
                } else if (contexts instanceof Map) {
                    Map hosts = (Map) contexts;
                    contexts = hosts.get("*");
                    contexts = LazyList.add(contexts, branches[b]);
                    hosts.put("*", contexts);
                } else {
                    contexts = LazyList.add(contexts, branches[b]);
                    contextMap.put(contextPath, contexts);
                }
            }
        }

        this._contextMap = contextMap;
    }

    public void setHandlers(Handler[] handlers) {
        this._contextMap = null;
        super.setHandlers(handlers);
        if (this.isStarted()) {
            this.mapContexts();
        }

    }

    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        Handler[] handlers = super.getHandlers();
        if (handlers != null && handlers.length != 0) {
            AsyncContinuation async = baseRequest.getAsyncContinuation();
            if (async.isAsync()) {
                ContextHandler map = async.getContextHandler();
                if (map != null) {
                    map.handle(target, baseRequest, request, response);
                    return;
                }
            }

            PathMap var16 = this._contextMap;
            if (var16 != null && target != null && target.startsWith("/")) {
                // To make sure every time click the "App ID" on HistoryPage, the SparkUI will be refreshed.
                if(target.matches("^/history/application_[^/]+")) {
                    if(flag == 0) {
                        Handler handler = (Handler) var16.get("/history/*");
                        handler.handle(target, baseRequest, request, response);
                        flag = 1;
                        return;
                    }
                    if(flag == 1) {
                        Handler handler = (Handler) var16.get(target + "/*");
                        handler.handle(target, baseRequest, request, response);
                        flag = 0;
                        return;
                    }
                }
                else {
                    Object var17 = var16.getLazyMatches(target);

                    for (int i1 = 0; i1 < LazyList.size(var17); ++i1) {
                        Map.Entry entry = (Map.Entry) LazyList.get(var17, i1);
                        Object list = entry.getValue();
                        if (!(list instanceof Map)) {
                            for (int var18 = 0; var18 < LazyList.size(list); ++var18) {
                                Handler var19 = (Handler) LazyList.get(list, var18);
                                var19.handle(target, baseRequest, request, response);
                                if (baseRequest.isHandled()) {
                                    return;
                                }
                            }
                        } else {
                            Map j = (Map) list;
                            String handler = this.normalizeHostname(request.getServerName());
                            list = j.get(handler);

                            int j1;
                            Handler handler1;
                            for (j1 = 0; j1 < LazyList.size(list); ++j1) {
                                handler1 = (Handler) LazyList.get(list, j1);
                                handler1.handle(target, baseRequest, request, response);
                                if (baseRequest.isHandled()) {
                                    return;
                                }
                            }

                            list = j.get("*." + handler.substring(handler.indexOf(".") + 1));

                            for (j1 = 0; j1 < LazyList.size(list); ++j1) {
                                handler1 = (Handler) LazyList.get(list, j1);
                                handler1.handle(target, baseRequest, request, response);
                                if (baseRequest.isHandled()) {
                                    return;
                                }
                            }

                            list = j.get("*");

                            for (j1 = 0; j1 < LazyList.size(list); ++j1) {
                                handler1 = (Handler) LazyList.get(list, j1);
                                handler1.handle(target, baseRequest, request, response);
                                if (baseRequest.isHandled()) {
                                    return;
                                }
                            }
                        }
                    }
                }
            } else {
                for (int i = 0; i < handlers.length; ++i) {
                    handlers[i].handle(target, baseRequest, request, response);
                    if (baseRequest.isHandled()) {
                        return;
                    }
                }
            }
        }
    }

    private String normalizeHostname(String host) {
        return host == null ? null : (host.endsWith(".") ? host.substring(0, host.length() - 1) : host);
    }
}
