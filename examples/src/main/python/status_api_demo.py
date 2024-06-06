#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
import threading
import queue as Queue
from typing import Any, Callable, List, Tuple

from pyspark import SparkConf, SparkContext


def delayed(seconds: int) -> Callable[[Any], Any]:
    def f(x: int) -> int:
        time.sleep(seconds)
        return x
    return f


def call_in_background(f: Callable[..., Any], *args: Any) -> Queue.Queue:
    result: Queue.Queue = Queue.Queue(1)
    t = threading.Thread(target=lambda: result.put(f(*args)))
    t.daemon = True
    t.start()
    return result


def main() -> None:
    conf = SparkConf().set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(appName="PythonStatusAPIDemo", conf=conf)

    def run() -> List[Tuple[int, int]]:
        rdd = sc.parallelize(range(10), 10).map(delayed(2))
        reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        return reduced.map(delayed(2)).collect()

    result = call_in_background(run)
    status = sc.statusTracker()
    while result.empty():
        ids = status.getJobIdsForGroup()
        for id in ids:
            job = status.getJobInfo(id)
            assert job is not None

            print("Job", id, "status: ", job.status)
            for sid in job.stageIds:
                info = status.getStageInfo(sid)
                if info:
                    print("Stage %d: %d tasks total (%d active, %d complete)" %
                          (sid, info.numTasks, info.numActiveTasks, info.numCompletedTasks))
        time.sleep(1)

    print("Job results are:", result.get())
    sc.stop()


if __name__ == "__main__":
    main()
