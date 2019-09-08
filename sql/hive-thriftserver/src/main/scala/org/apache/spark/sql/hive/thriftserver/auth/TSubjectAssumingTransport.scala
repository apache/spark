/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.auth

import java.security.{AccessController, PrivilegedActionException, PrivilegedExceptionAction}
import javax.security.auth.Subject

import org.apache.hadoop.hive.thrift.TFilterTransport
import org.apache.thrift.transport.{TTransport, TTransportException}

class TSubjectAssumingTransport(wrapped: TTransport) extends TFilterTransport(wrapped) {
  @throws[TTransportException]
  override def open(): Unit = {
    try {
      val context = AccessController.getContext
      val subject = Subject.getSubject(context)
      Subject.doAs(subject, new PrivilegedExceptionAction[Void]() {
        override def run: Void = {
          try
            wrapped.open()
          catch {
            case tte: TTransportException =>
              // Wrap the transport exception in an RTE, since Subject.doAs() then goes
              // and unwraps this for us out of the doAs block. We then unwrap one
              // more time in our catch clause to get back the TTE. (ugh)
              throw new RuntimeException(tte)
          }
          null
        }
      })
    } catch {
      case ioe: PrivilegedActionException =>
        throw new RuntimeException("Received an ioe we never threw!", ioe)
      case rte: RuntimeException =>
        if (rte.getCause.isInstanceOf[TTransportException]) {
          throw rte.getCause.asInstanceOf[TTransportException]
        } else {
          throw rte
        }
    }
  }
}
