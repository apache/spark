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

package org.apache.spark

import java.util.concurrent.{ExecutionException, TimeUnit, Future}

import org.apache.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

class FutureJob[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: () => T)
  extends Future[T] {

  override def isDone: Boolean = jobWaiter.jobFinished

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
    jobWaiter.kill()
    true
  }

  override def isCancelled: Boolean = {
    throw new UnsupportedOperationException
  }

  override def get(): T = {
    jobWaiter.awaitResult() match {
      case JobSucceeded =>
        resultFunc()
      case JobFailed(e: Exception, _) =>
        throw new ExecutionException(e)
    }
  }

  override def get(timeout: Long, unit: TimeUnit): T = {
    throw new UnsupportedOperationException
  }
}
