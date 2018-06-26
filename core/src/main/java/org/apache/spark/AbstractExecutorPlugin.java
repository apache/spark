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

package org.apache.spark;

import org.apache.spark.annotation.DeveloperApi;

/**
 * A plugin which can be automaticaly instantiated within each Spark executor.  Users can specify
 * plugins which should be created with the "spark.executor.plugins" configuration.  An instance
 * of each plugin will be created for every executor, including those created by dynamic allocation,
 * before the executor starts running any tasks.
 *
 * The specific api exposed to the end users still considered to be very unstable.  If implementors
 * extend this base class, we will *hopefully* be able to keep compatability by providing dummy
 * implementations for any methods added, but make no guarantees this will always be possible across
 * all spark releases.
 *
 * Spark does nothing to verify the plugin is doing legitimate things, or to manage the resources
 * it uses.  A plugin acquires the same privileges as the user running the task.  A bad plugin
 * could also intefere with task execution and make the executor fail in unexpected ways.
 */
@DeveloperApi
public class AbstractExecutorPlugin {
}
