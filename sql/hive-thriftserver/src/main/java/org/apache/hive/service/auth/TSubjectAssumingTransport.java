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

package org.apache.hive.service.auth;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;

import org.apache.hadoop.hive.thrift.TFilterTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * This is used on the client side, where the API explicitly opens a transport to
 * the server using the Subject.doAs().
 */
public class TSubjectAssumingTransport extends TFilterTransport {

  public TSubjectAssumingTransport(TTransport wrapped) {
    super(wrapped);
  }

  @Override
  public void open() throws TTransportException {
    try {
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
        try {
          wrapped.open();
        } catch (TTransportException tte) {
          // Wrap the transport exception in an RTE, since Subject.doAs() then goes
          // and unwraps this for us out of the doAs block. We then unwrap one
          // more time in our catch clause to get back the TTE. (ugh)
          throw new RuntimeException(tte);
        }
        return null;
      });
    } catch (PrivilegedActionException ioe) {
      throw new RuntimeException("Received an ioe we never threw!", ioe);
    } catch (RuntimeException rte) {
      if (rte.getCause() instanceof TTransportException) {
        throw (TTransportException) rte.getCause();
      } else {
        throw rte;
      }
    }
  }

}
