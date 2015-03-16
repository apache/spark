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

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sort <file>"
        exit(-1)
    sc = SparkContext(appName="PythonSort")
    lines = sc.textFile(sys.argv[1], 1)
    sortedCount = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (int(x), 1)) \
        .sortByKey(lambda x: x)
    # This is just a demo on how to bring all the sorted data back to a single node.
    # In reality, we wouldn't want to collect all the data to the driver node.
    output = sortedCount.collect()
    for (num, unitcount) in output:
        print num

    sc.stop()
