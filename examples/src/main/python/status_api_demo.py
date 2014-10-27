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
import Queue

from pyspark import SparkContext


def delayed(seconds):
    def f(x):
        time.sleep(seconds)
        return x
    return f


def call_in_background(f, *args):
    result = Queue.Queue(1)
    t = threading.Thread(target=lambda: result.put(f(*args)))
    t.daemon = True
    t.start()
    return result


def main():
    sc = SparkContext(appName="PythonStatusAPIDemo")

    def run():
        sc.setJobGroup("demo", "demo status api")
        rdd = sc.parallelize(range(10), 10).map(delayed(2))
        reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        return reduced.map(delayed(2)).collect()

    result = call_in_background(run)

    while result.empty():
        ids = sc.getJobIdsForGroup("demo")
        for id in ids:
            job = sc.getJobInfo(id)
            print "Job", id, "status: ", job.status()
            for sid in job.stageIds():
                info = sc.getStageInfo(sid)
                if info:
                    print "Stage %d: %d tasks total (%d active, %d complete)" % \
                          (sid, info.numTasks(), info.numActiveTasks(), info.numCompletedTasks())
        time.sleep(1)

    print "Job results are:", result.get()
    sc.stop()

if __name__ == "__main__":
    main()
