/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/***
 * This class contains a collection of task attempts and their states.
 */
public class TaskAttemptCollection {
  private Map<Long, TaskAttemptIdAndState> tasks = new HashMap<>();

  public TaskAttemptCollection() {
  }

  public TaskAttemptIdAndState getTask(Long taskAttemptId) {
    TaskAttemptIdAndState task = tasks.get(taskAttemptId);
    if (task == null) {
      task = new TaskAttemptIdAndState(taskAttemptId);
      tasks.put(taskAttemptId, task);
    }
    return task;
  }

  public List<Long> getCommittedTaskIds() {
    return tasks.values().stream().filter(t -> t.isCommitted()).map(t -> t.getTaskAttemptId())
        .collect(Collectors.toList());
  }
}
