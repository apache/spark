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


import os
import sys

import viztracer
from viztracer.main import main

import pyspark.worker


def viztracer_wrapper(func):

    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        tracer = viztracer.get_tracer()
        if tracer is not None:
            tracer.exit_routine()
        return result
    return wrapper


if __name__ == "__main__":
    pyspark.worker.main = viztracer_wrapper(pyspark.worker.main)

    if os.getenv("SPARK_VIZTRACER_OUTPUT_DIR") is not None:
        output_dir = os.getenv("SPARK_VIZTRACER_OUTPUT_DIR")
    else:
        output_dir = "./"

    sys.argv[:] = ["viztracer", "-m", "pyspark.daemon", "--quiet", "-u",
                   "--output_dir", output_dir]
    main()
