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

package org.apache.spark.deploy.history.yarn

import org.apache.spark.{Logging, SparkContext, SparkFirehoseListener}
import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Spark listener which queues up all received events to the [[YarnHistoryService]] passed
 * as a constructor. There's no attempt to filter event types at this point.
 *
 * @param sc context
 * @param service service to forward events to
 */
private[spark] class YarnEventListener(sc: SparkContext, service: YarnHistoryService)
  extends SparkFirehoseListener with Logging {

  /**
   * queue the event with the service, timestamped to the current time.
   *
   * @param event event to queue
   */
  override def onEvent(event: SparkListenerEvent): Unit = {
    service.enqueue(event)
  }

}
