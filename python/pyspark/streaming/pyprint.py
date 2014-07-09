<<<<<<< HEAD
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
from itertools import chain

from pyspark.serializers import PickleSerializer


def collect(binary_file_path):
    """
    Read pickled file written by SparkStreaming
    """
=======
import sys
from itertools import chain
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer

def collect(binary_file_path):
>>>>>>> initial commit for pySparkStreaming
    dse = PickleSerializer()
    with open(binary_file_path, 'rb') as tempFile:
        for item in dse.load_stream(tempFile):
            yield item
<<<<<<< HEAD


=======
>>>>>>> initial commit for pySparkStreaming
def main():
    try:
        binary_file_path = sys.argv[1]
    except:
<<<<<<< HEAD
        print "Missed FilePath in argements"
=======
        print "Missed FilePath in argement"
>>>>>>> initial commit for pySparkStreaming

    if not binary_file_path:
        return 

    counter = 0
    for rdd in chain.from_iterable(collect(binary_file_path)):
        print rdd
        counter = counter + 1
        if counter >= 10:
            print "..."
            break

<<<<<<< HEAD

=======
>>>>>>> initial commit for pySparkStreaming
if __name__ =="__main__":
    exit(main())
