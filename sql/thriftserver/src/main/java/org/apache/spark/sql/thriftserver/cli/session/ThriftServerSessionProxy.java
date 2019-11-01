/*
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

package org.apache.spark.sql.thriftserver.cli.session;

/**
 * Proxy wrapper on HiveSession to execute operations
 * by impersonating given user
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.thriftserver.cli.SparkThriftServerSQLException;

public class ThriftServerSessionProxy implements InvocationHandler {
    private final ThriftServerSession base;
    private final UserGroupInformation ugi;

    public ThriftServerSessionProxy(ThriftServerSession hiveSession, UserGroupInformation ugi) {
        this.base = hiveSession;
        this.ugi = ugi;
    }

    public static ThriftServerSession getProxy(ThriftServerSession hiveSession, UserGroupInformation ugi)
            throws IllegalArgumentException, SparkThriftServerSQLException {
        return (ThriftServerSession) Proxy.newProxyInstance(ThriftServerSession.class.getClassLoader(),
                new Class<?>[]{ThriftServerSession.class},
                new ThriftServerSessionProxy(hiveSession, ugi));
    }

    @Override
    public Object invoke(Object arg0, final Method method, final Object[] args)
            throws Throwable {
        try {
            if (method.getDeclaringClass() == ThriftServerSessionBase.class) {
                return invoke(method, args);
            }
            return ugi.doAs(
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws SparkThriftServerSQLException {
                            return invoke(method, args);
                        }
                    });
        } catch (UndeclaredThrowableException e) {
            Throwable innerException = e.getCause();
            if (innerException instanceof PrivilegedActionException) {
                throw innerException.getCause();
            } else {
                throw e.getCause();
            }
        }
    }

    private Object invoke(final Method method, final Object[] args) throws SparkThriftServerSQLException {
        try {
            return method.invoke(base, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof SparkThriftServerSQLException) {
                throw (SparkThriftServerSQLException) e.getCause();
            } else if (e.getCause() instanceof OutOfMemoryError) {
                throw (OutOfMemoryError) e.getCause();
            } else if (e.getCause() instanceof Error) {
                // TODO: maybe we should throw this as-is too. ThriftCLIService currently catches Exception,
                //       so the combination determines what would kill the HS2 executor thread. For now,
                //       let's only allow OOM to propagate.
            }
            throw new RuntimeException(e.getCause());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}

