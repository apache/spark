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
package org.apache.hadoop.io.retry;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ipc.RemoteException;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  /**
   * <p>
   * Try once, and fail by re-throwing the exception.
   * This corresponds to having no retry mechanism in place.
   * </p>
   */
  public static final RetryPolicy TRY_ONCE_THEN_FAIL = new TryOnceThenFail();
  
  /**
   * <p>
   * Try once, and fail silently for <code>void</code> methods, or by
   * re-throwing the exception for non-<code>void</code> methods.
   * </p>
   */
  public static final RetryPolicy TRY_ONCE_DONT_FAIL = new TryOnceDontFail();
  
  /**
   * <p>
   * Keep trying forever.
   * </p>
   */
  public static final RetryPolicy RETRY_FOREVER = new RetryForever();
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying for a maximum time, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by the number of tries so far.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by a random
   * number in the range of [0, 2 to the number of retries)
   * </p>
   */
  public static final RetryPolicy exponentialBackoffRetry(
      int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final RetryPolicy retryByException(RetryPolicy defaultPolicy,
                                                   Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
    return new ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  /**
   * <p>
   * A retry policy for RemoteException
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final RetryPolicy retryByRemoteException(
      RetryPolicy defaultPolicy,
      Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
    return new RemoteExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  static class TryOnceThenFail implements RetryPolicy {
    public boolean shouldRetry(Exception e, int retries) throws Exception {
      throw e;
    }
  }
  static class TryOnceDontFail implements RetryPolicy {
    public boolean shouldRetry(Exception e, int retries) throws Exception {
      return false;
    }
  }
  
  static class RetryForever implements RetryPolicy {
    public boolean shouldRetry(Exception e, int retries) throws Exception {
      return true;
    }
  }
  
  static abstract class RetryLimited implements RetryPolicy {
    int maxRetries;
    long sleepTime;
    TimeUnit timeUnit;
    
    public RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    public boolean shouldRetry(Exception e, int retries) throws Exception {
      if (retries >= maxRetries) {
        throw e;
      }
      try {
        timeUnit.sleep(calculateSleepTime(retries));
      } catch (InterruptedException ie) {
        // retry
      }
      return true;
    }
    
    protected abstract long calculateSleepTime(int retries);
  }
  
  static class RetryUpToMaximumCountWithFixedSleep extends RetryLimited {
    public RetryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime;
    }
  }
  
  static class RetryUpToMaximumTimeWithFixedSleep extends RetryUpToMaximumCountWithFixedSleep {
    public RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit timeUnit) {
      super((int) (maxTime / sleepTime), sleepTime, timeUnit);
    }
  }
  
  static class RetryUpToMaximumCountWithProportionalSleep extends RetryLimited {
    public RetryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime * (retries + 1);
    }
  }
  
  static class ExceptionDependentRetry implements RetryPolicy {

    RetryPolicy defaultPolicy;
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap;
    
    public ExceptionDependentRetry(RetryPolicy defaultPolicy,
                                   Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionToPolicyMap = exceptionToPolicyMap;
    }

    public boolean shouldRetry(Exception e, int retries) throws Exception {
      RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries);
    }
    
  }
  
  static class RemoteExceptionDependentRetry implements RetryPolicy {

    RetryPolicy defaultPolicy;
    Map<String, RetryPolicy> exceptionNameToPolicyMap;
    
    public RemoteExceptionDependentRetry(RetryPolicy defaultPolicy,
                                   Map<Class<? extends Exception>,
                                   RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionNameToPolicyMap = new HashMap<String, RetryPolicy>();
      for (Entry<Class<? extends Exception>, RetryPolicy> e :
          exceptionToPolicyMap.entrySet()) {
        exceptionNameToPolicyMap.put(e.getKey().getName(), e.getValue());
      }
    }

    public boolean shouldRetry(Exception e, int retries) throws Exception {
      RetryPolicy policy = null;
      if (e instanceof RemoteException) {
        policy = exceptionNameToPolicyMap.get(
            ((RemoteException) e).getClassName());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries);
    }
  }
  
  static class ExponentialBackoffRetry extends RetryLimited {
    private Random r = new Random();
    public ExponentialBackoffRetry(
        int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime*r.nextInt(1<<(retries+1));
    }
  }
}
