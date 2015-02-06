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


import sys
import time
from operator import add

from pyspark import SparkContext,SparkConf

#Usage: BroadcastTest [slices] [numElem] [broadcastAlgo] [blockSize]

if __name__ == "__main__":

        slices  =  int(sys.argv[0]) if len(sys.argv) > 1 else 1
        num  =  int(sys.argv[1]) if len(sys.argv) > 2 else 10000000
        bcName = sys.argv[2] if len(sys.argv) > 3 else "Http"
        blockSize =  sys.argv[3] if len(sys.argv) > 4 else "4092"

        conf = SparkConf().setAppName("Broadcast Test") \
                          .setMaster("local") \
                          .set("spark.broadcast.factory", "org.apache.spark.broadcast.%sBroadcastFactory"%bcName) \
                          .set("spark.broadcast.blockSize", blockSize)



        sc = SparkContext(conf=conf)
        #simple broadcast       
        b = sc.broadcast([1, 2, 3])
        result =sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
        for s in result:
            print "value is %s:" % s
        #large broadcast
        for i in range(3):
            print "Iteration %i" % i
            start = time.time()
            barr1 = sc.broadcast(range(num))
            observedsize = sc.parallelize(range(10),slices).map(lambda x: len(barr1.value)).collect()
            end = time.time()
            print end - start
        sc.stop()