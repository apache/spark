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
package org.apache.hadoop.metrics.spi;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

public class CompositeContext extends AbstractMetricsContext {

  private static final Log LOG = LogFactory.getLog(CompositeContext.class);
  private static final String ARITY_LABEL = "arity";
  private static final String SUB_FMT = "%s.sub%d";
  private final ArrayList<MetricsContext> subctxt =
    new ArrayList<MetricsContext>();

  public CompositeContext() {
  }

  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
    int nKids;
    try {
      String sKids = getAttribute(ARITY_LABEL);
      nKids = Integer.valueOf(sKids);
    } catch (Exception e) {
      LOG.error("Unable to initialize composite metric " + contextName +
                ": could not init arity", e);
      return;
    }
    for (int i = 0; i < nKids; ++i) {
      MetricsContext ctxt = MetricsUtil.getContext(
          String.format(SUB_FMT, contextName, i), contextName);
      if (null != ctxt) {
        subctxt.add(ctxt);
      }
    }
  }

  @Override
  public MetricsRecord newRecord(String recordName) {
    return (MetricsRecord) Proxy.newProxyInstance(
        MetricsRecord.class.getClassLoader(),
        new Class[] { MetricsRecord.class },
        new MetricsRecordDelegator(recordName, subctxt));
  }

  @Override
  protected void emitRecord(String contextName, String recordName,
      OutputRecord outRec) throws IOException {
    for (MetricsContext ctxt : subctxt) {
      try {
        ((AbstractMetricsContext)ctxt).emitRecord(
          contextName, recordName, outRec);
        if (contextName == null || recordName == null || outRec == null) {
          throw new IOException(contextName + ":" + recordName + ":" + outRec);
        }
      } catch (IOException e) {
        LOG.warn("emitRecord failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @Override
  protected void flush() throws IOException {
    for (MetricsContext ctxt : subctxt) {
      try {
        ((AbstractMetricsContext)ctxt).flush();
      } catch (IOException e) {
        LOG.warn("flush failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @Override
  public void startMonitoring() throws IOException {
    for (MetricsContext ctxt : subctxt) {
      try {
        ctxt.startMonitoring();
      } catch (IOException e) {
        LOG.warn("startMonitoring failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @Override
  public void stopMonitoring() {
    for (MetricsContext ctxt : subctxt) {
      ctxt.stopMonitoring();
    }
  }

  /**
   * Return true if all subcontexts are monitoring.
   */
  @Override
  public boolean isMonitoring() {
    boolean ret = true;
    for (MetricsContext ctxt : subctxt) {
      ret &= ctxt.isMonitoring();
    }
    return ret;
  }

  @Override
  public void close() {
    for (MetricsContext ctxt : subctxt) {
      ctxt.close();
    }
  }

  @Override
  public void registerUpdater(Updater updater) {
    for (MetricsContext ctxt : subctxt) {
      ctxt.registerUpdater(updater);
    }
  }

  @Override
  public void unregisterUpdater(Updater updater) {
    for (MetricsContext ctxt : subctxt) {
      ctxt.unregisterUpdater(updater);
    }
  }

  private static class MetricsRecordDelegator implements InvocationHandler {
    private static final Method m_getRecordName = initMethod();
    private static Method initMethod() {
      try {
        return MetricsRecord.class.getMethod("getRecordName", new Class[0]);
      } catch (Exception e) {
        throw new RuntimeException("Internal error", e);
      }
    }

    private final String recordName;
    private final ArrayList<MetricsRecord> subrecs;

    MetricsRecordDelegator(String recordName, ArrayList<MetricsContext> ctxts) {
      this.recordName = recordName;
      this.subrecs = new ArrayList<MetricsRecord>(ctxts.size());
      for (MetricsContext ctxt : ctxts) {
        subrecs.add(ctxt.createRecord(recordName));
      }
    }

    public Object invoke(Object p, Method m, Object[] args) throws Throwable {
      if (m_getRecordName.equals(m)) {
        return recordName;
      }
      assert Void.TYPE.equals(m.getReturnType());
      for (MetricsRecord rec : subrecs) {
        m.invoke(rec, args);
      }
      return null;
    }
  }

}
