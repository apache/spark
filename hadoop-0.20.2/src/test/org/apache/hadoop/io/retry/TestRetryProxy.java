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

import static org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER;
import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_DONT_FAIL;
import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByRemoteException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithProportionalSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumTimeWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.io.retry.UnreliableInterface.FatalException;
import org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException;
import org.apache.hadoop.ipc.RemoteException;

public class TestRetryProxy extends TestCase {
  
  private UnreliableImplementation unreliableImpl;
  
  @Override
  protected void setUp() throws Exception {
    unreliableImpl = new UnreliableImplementation();
  }

  public void testTryOnceThenFail() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, TRY_ONCE_THEN_FAIL);
    unreliable.alwaysSucceeds();
    try {
      unreliable.failsOnceThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testTryOnceDontFail() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, TRY_ONCE_DONT_FAIL);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsOnceThenSucceedsWithReturnValue();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testRetryForever() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    unreliable.failsTenTimesThenSucceeds();
  }
  
  public void testRetryUpToMaximumCountWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumCountWithFixedSleep(8, 1, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testRetryUpToMaximumTimeWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumTimeWithFixedSleep(80, 10, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testRetryUpToMaximumCountWithProportionalSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumCountWithProportionalSleep(8, 1, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testExponentialRetry() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        exponentialBackoffRetry(5, 1L, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testRetryByException() throws UnreliableException {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(FatalException.class, TRY_ONCE_THEN_FAIL);
    
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryByException(RETRY_FOREVER, exceptionToPolicyMap));
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.alwaysFailsWithFatalException();
      fail("Should fail");
    } catch (FatalException e) {
      // expected
    }
  }
  
  public void testRetryByRemoteException() throws UnreliableException {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(FatalException.class, TRY_ONCE_THEN_FAIL);
    
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryByRemoteException(RETRY_FOREVER, exceptionToPolicyMap));
    try {
      unreliable.alwaysFailsWithRemoteFatalException();
      fail("Should fail");
    } catch (RemoteException e) {
      // expected
    }
  }  
  
}
