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
import os
from pyspark import SparkContext
from mylib import myfunc

# this is exmaple for using ourself module file
if __name__ == "__main__":

    """
        Usage:module.py
          bin/spark-submit examples/src/main/python/module.py
        use   --py-files  to  replace addPyFile()
          bin/spark-submit  examples/src/main/python/module.py  --py-files  examples/src/main/python/mylib.zip
    """

    tmpdir = os.path.split(sys.argv[0])[0]
    sc = SparkContext(appName="PythonModule")
    path = os.path.join(tmpdir, "mylib.zip")
    sc.addPyFile(path)
	print "use mylib.myfunc at mylib.zip to process"
    print sc.parallelize([1, 2, 3]) \
			.map(myfunc)            \
			.collect()
		