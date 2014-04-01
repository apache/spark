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

package org.apache.hadoop.io.retry;

import org.apache.hadoop.ipc.RemoteException;

public class UnreliableImplementation implements UnreliableInterface {

  private int failsOnceInvocationCount,
    failsOnceWithValueInvocationCount,
    failsTenTimesInvocationCount;
  
  public void alwaysSucceeds() {
    // do nothing
  }
  
  public void alwaysFailsWithFatalException() throws FatalException {
    throw new FatalException();
  }
  
  public void alwaysFailsWithRemoteFatalException() throws RemoteException {
    throw new RemoteException(FatalException.class.getName(), "Oops");
  }

  public void failsOnceThenSucceeds() throws UnreliableException {
    if (failsOnceInvocationCount++ == 0) {
      throw new UnreliableException();
    }
  }

  public boolean failsOnceThenSucceedsWithReturnValue() throws UnreliableException {
    if (failsOnceWithValueInvocationCount++ == 0) {
      throw new UnreliableException();
    }
    return true;
  }

  public void failsTenTimesThenSucceeds() throws UnreliableException {
    if (failsTenTimesInvocationCount++ < 10) {
      throw new UnreliableException();
    }
  }

}
