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
package org.apache.spark.deploy.history

/**
 * Contains the classes needed to listen to spark events and publish them to a YARN application
 * timeline service.
 *
 * How it works
 *
 * 1. `YarnEventListener` subscribes to events in the current spark context.
 *
 * 2. These are forwarded to an instance of `YarnHistoryService`.
 *
 * 3. This, if enabled, publishes events to the configured ATS server.
 *
 * 4. The Spark History Service, is configured to use `YarnHistoryProvider`
 * as its provider of history information.
 *
 * 5. It enumerates application instances and attempts, for display in the web UI and access
 * via the REST API.
 *
 * 6. When details of a specific attempt is requested, it is retrieved from the ATS server.
 *
 * See: [[http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/TimelineServer.html]]
 */
package object yarn {

}
