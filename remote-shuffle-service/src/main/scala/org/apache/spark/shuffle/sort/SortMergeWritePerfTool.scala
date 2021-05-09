/*
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

package org.apache.spark.shuffle.sort

import java.util

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.common._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.internal.ShuffleWritePerfTool
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents

class SortMergeWritePerfTool extends ShuffleWritePerfTool with Logging {

  override def createShuffleWriter(
      shuffleId: Int,
      shuffleDependency: ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]],
      appMapId: AppMapId,
      taskAttemptId: Long):
    ShuffleWriter[Array[Byte], Array[Byte]] = {
    val sparkConf = new SparkConf(false)
    val handle = new BaseShuffleHandle[Array[Byte], Array[Byte], Array[Byte]](
      shuffleId,
      shuffleDependency)
    val shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(sparkConf)
    shuffleExecutorComponents.initializeExecutor(appMapId.getAppId,
      "1",
      new util.HashMap[String, String]())
    new SortShuffleWriter(
      handle = handle,
      mapId = appMapId.getMapId,
      context = new MockTaskContext(0, 0, taskAttemptId),
      shuffleExecutorComponents = shuffleExecutorComponents
    )
  }

}

object SortMergeWritePerfTool extends Logging {

  def main(args: Array[String]): Unit = {
    runOnce()
  }

  private def runOnce(): Unit = {
    val tool = new SortMergeWritePerfTool()
    try {
      tool.setup()
      tool.run()
    } finally {
      tool.cleanup()
    }
  }
}