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

import org.apache.spark.annotation.{Experimental, Since}

/** A [[TaskContext]] with extra info and tooling for a barrier stage. */
trait BarrierTaskContext extends TaskContext {

  /**
   * :: Experimental ::
   * Sets a global barrier and waits until all tasks in this stage hit this barrier. Similar to
   * MPI_Barrier function in MPI, the barrier() function call blocks until all tasks in the same
   * stage have reached this routine.
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): Unit

  /**
   * :: Experimental ::
   * Returns the all task infos in this barrier stage, the task infos are ordered by partitionId.
   */
  @Experimental
  @Since("2.4.0")
  def getTaskInfos(): Array[BarrierTaskInfo]
}
