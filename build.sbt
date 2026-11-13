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

val isCI = sys.env.contains("GITHUB_ACTIONS")

// Build Scans only: the sbt plugin cannot exclude test tasks from the build cache, and Spark
// tests read inputs the cache key does not track, so caching stays off rather than risk
// skipping a failing test.
ThisBuild / develocityConfiguration ~= { previous =>
  previous
    .withProjectId(ProjectId("spark"))
    .withServer(
      previous.server
        .withUrl(url("https://develocity.apache.org")))
    .withBuildScan(
      previous.buildScan
        .withPublishing(Publishing.onlyIf(_.authenticated))
        .withBackgroundUpload(!isCI))
    .withBuildCache(
      previous.buildCache
        .withLocal(previous.buildCache.local.withEnabled(false))
        .withRemote(previous.buildCache.remote.withEnabled(false)))
}
